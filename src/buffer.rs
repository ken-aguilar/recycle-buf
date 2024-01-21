use std::{
    ops::{Deref, DerefMut},
    ptr::NonNull,
    sync::{
        atomic::{AtomicU32, Ordering},
        Arc, Condvar, Mutex,
    },
    time::Duration,
};

use crate::sync_cell::SyncUnsafeCell;
use crate::ordering::BufferState;

pub type NotNullItem<T> = NonNull<ItemCounter<T>>;
pub type ContainerType<T> = *const ItemCounter<T>;
pub type GuardedBufferPtr<B> = Arc<SyncUnsafeCell<B>>;

pub type DynamicBufferPtr<T> = GuardedBufferPtr<DynamicBuffer<T>>;
pub type StaticBufferPtr<T, const N: usize> = GuardedBufferPtr<StaticBuffer<T, N>>;

pub fn make_container<T>(item: T) -> ContainerType<T> {
    // move the item to the heap via box
    let boxed = ItemCounter {
        item,
        ref_counter: AtomicU32::new(0),
    }
    .into_box();
    // get a pointer from the heap
    Box::into_raw(boxed)
}

pub struct ItemCounter<T> {
    pub item: T,
    ref_counter: AtomicU32,
}

unsafe impl<T> Send for ItemCounter<T> where T: Send {}
unsafe impl<T> Sync for ItemCounter<T> where T: Sync {}

impl<T> ItemCounter<T> {
    #[inline]
    fn into_box(self) -> Box<Self> {
        Box::new(self)
    }

    #[inline]
    pub fn increment(&self) -> u32 {
        self.ref_counter.fetch_add(1, Ordering::Relaxed)
    }

    #[inline]
    pub fn decrement(&self) -> u32 {
        self.ref_counter.fetch_sub(1, Ordering::Relaxed)
    }
}

pub trait RecyclerBuffer {
    type ItemType;
    const NULL_ITEM: *const ItemCounter<Self::ItemType> = std::ptr::null();
    fn available(&self) -> usize;
    fn empty(&self) -> bool;
    fn capacity(&self) -> usize;
}

pub trait StackBuffer: RecyclerBuffer {
    fn get_one(&mut self) -> Option<ContainerType<Self::ItemType>>;
    fn wait_for_one(&mut self) -> ContainerType<Self::ItemType>;
    fn timed_wait_for_one(&mut self, wait_time: Duration) -> Option<ContainerType<Self::ItemType>>;
    fn recycle(&mut self, item: NotNullItem<Self::ItemType>);
}

pub trait ProducerConsumerBuffer: RecyclerBuffer {
    const MAX: usize;

    fn consume_at(&self, index: &mut usize) -> (ContainerType<Self::ItemType>, usize);
    fn broadcast<F>(&mut self, f: F)
    where
        F: FnOnce(&mut Self::ItemType);
    fn shutdown(&mut self);
    fn drop_consumer(&mut self);
    fn recycle(&mut self, index_to_recycle: usize);
    fn consume_start(&mut self) -> usize;
}

#[derive(Debug)]
pub struct DynamicBuffer<T> {
    data: Box<[ContainerType<T>]>,
    index: usize,
    pub mutex: Mutex<()>,
    pub recycled_event: Condvar,
}

impl<T> DynamicBuffer<T> {
    pub fn new(data: Box<[ContainerType<T>]>) -> Self {

        Self {
            data,
            index: 0,
            mutex: Mutex::new(()),
            recycled_event: Condvar::new(),
        }
    }
}

unsafe impl<T> Send for DynamicBuffer<T> where T: Send {}

impl<T> Drop for DynamicBuffer<T> {
    fn drop(&mut self) {
        // we have to manually drop each item in the buffer
        self.data
            .iter()
            .for_each(|ptr: &*const ItemCounter<T>| unsafe {
                drop(Box::from_raw(ptr.cast_mut()));
            })
    }
}

impl<T> RecyclerBuffer for DynamicBuffer<T> {
    type ItemType = T;

    #[inline]
    fn available(&self) -> usize {
        self.data.len() - self.index
    }

    #[inline]
    fn empty(&self) -> bool {
        self.index == self.data.len()
    }

    #[inline]
    fn capacity(&self) -> usize {
        self.data.len()
    }

}

impl<T> StackBuffer for DynamicBuffer<T> {
    #[inline]
    /// takes an item from the internal buffer
    fn wait_for_one(&mut self) -> ContainerType<T> {
        // while empty wait until one is available
        let _lock = self
            .recycled_event
            .wait_while(self.mutex.lock().unwrap(), |_b| self.empty())
            .unwrap();

        let item = self.data[self.index];
        self.index += 1;
        //*item
        item
    }

    fn timed_wait_for_one(&mut self, wait_time: Duration) -> Option<ContainerType<Self::ItemType>> {
        // loop until one is available
        if let Ok((mut _lock, timeout)) =
            self.recycled_event
                .wait_timeout_while(self.mutex.lock().unwrap(), wait_time, |_e| self.empty())
        {
            if !timeout.timed_out() {
                // we didn't time out so at least one item is available
                let item = self.data[self.index];
                self.index += 1;

                return Some(item);
            }
        }
        None
    }

    fn get_one(&mut self) -> Option<ContainerType<Self::ItemType>> {
        let _lock = self.mutex.lock().unwrap();
        if self.empty() {
            // empty
            None
        } else {
            let item = self.data[self.index];
            self.index += 1;
            //*item
            Some(item)
        }
    }

    #[inline]
    fn recycle(&mut self, item: NotNullItem<T>) {
        let _lock = self.mutex.lock();

        self.index -= 1;
        //*unsafe {self.data.get_unchecked_mut(self.index) } = item
        //println!("recycling into index {}", self.index);
        self.data[self.index] = item.as_ptr();
        self.recycled_event.notify_one();
    }
}

#[derive(Debug)]
pub struct StaticBuffer<T, const SIZE: usize>
where
    T: Send + Sync,
{
    data: [ContainerType<T>; SIZE],
    index: usize,
    consumer_count: u32,
    pub mutex: Mutex<()>,
    pub recycled_event: Condvar,
    pub item_produced_event: Condvar,
    recycle_index: usize,
}

unsafe impl<T, const SIZE: usize> Send for StaticBuffer<T, SIZE> where T: Send + Sync {}

impl<T, const SIZE: usize> StaticBuffer<T, SIZE>
where
    T: Send + Sync,
{
    pub fn new(data: [ContainerType<T>; SIZE]) -> Self {

        Self {
            data,
            index: SIZE-1,
            consumer_count: 0,
            mutex: Mutex::new(()),
            recycled_event: Condvar::new(),
            item_produced_event: Condvar::new(),
            recycle_index: SIZE,
        }
    }
}

impl<T, const SIZE: usize> Drop for StaticBuffer<T, SIZE>
where
    T: Send + Sync,
{
    fn drop(&mut self) {
        // we have to manually drop each item in the buffer
        self.data
            .iter()
            .for_each(|ptr: &*const ItemCounter<T>| unsafe {
                drop(Box::from_raw(ptr.cast_mut()));
            })
    }
}

impl<T, const SIZE: usize> RecyclerBuffer for StaticBuffer<T, SIZE>
where
    T: Send + Sync,
{
    type ItemType = T;

    #[inline]
    fn available(&self) -> usize {
        self.capacity() - self.recycle_index
    }

    #[inline]
    fn empty(&self) -> bool {
        self.index == self.recycle_index
    }

    #[inline]
    fn capacity(&self) -> usize {
        SIZE
    }

}

impl<T, const SIZE: usize> ProducerConsumerBuffer for StaticBuffer<T, SIZE>
where
    T: Send + Sync,
{

    const MAX: usize = SIZE;

    #[inline]
    fn recycle(&mut self, index_to_recyle: usize) {
        let _lock = self.mutex.lock();

        println!("recycling into index {}", index_to_recyle);
        if index_to_recyle  == 0 {
            self.recycle_index = SIZE;
            if self.index == SIZE {
                self.index -= 1;
            }
        } else {
            self.recycle_index = index_to_recyle - 1;
        }

        self.recycled_event.notify_one();
    }

    fn consume_at(&self, consumer_start: &mut usize) -> (ContainerType<Self::ItemType>, usize) {
        let lock = self
            .item_produced_event
            .wait_while(self.mutex.lock().unwrap(), |_e| {
                *consumer_start == self.index
            })
            .unwrap();

        let consuming_index = *consumer_start;

        let (item, recycle_to) = match consuming_index {
            _ if consuming_index == SIZE =>  {
                *consumer_start = SIZE-2;
                (self.data[SIZE-1], SIZE -1)
            }, 
            0 => {
                *consumer_start = SIZE;
                (self.data[0], 0)
            }
            _ => {
                let p = self.data[consuming_index];
                *consumer_start -= 1;
                (p, consuming_index)
            }
        };
        println!("consumed, next consumption at {}, will recycle to {}, limit is {}", *consumer_start, recycle_to, self.index);

        drop(lock);
        (item, recycle_to)
    }

    #[inline]
    fn broadcast<F>(&mut self, f: F)
    where
        F: FnOnce(&mut Self::ItemType),
    {
        // while empty wait until one is available
        let _lock = self
        .recycled_event
        .wait_while(self.mutex.lock().unwrap(), |_b| self.empty())
        .unwrap();
    
        let ptr = match self.index {
            _ if self.index == SIZE => {
                self.index = SIZE-2;
                println!("writing to index {}, recycler is at {}", SIZE-1,self.recycle_index);
                self.data[SIZE-1]
            }, 
            0 => {
                self.index = SIZE;
                println!("writing to index 0, recycler is at {}", self.recycle_index);

                self.data[0]
            }
            _ => {
                println!("writing to index {}, recycler is at {}", self.index,self.recycle_index);
                let p = self.data[self.index];
                self.index -= 1;
                p
            }
        };

        let item_counter = unsafe { ptr.cast_mut().as_mut().unwrap() };

        item_counter
            .ref_counter
            .store(self.consumer_count, Ordering::Relaxed);

        f(&mut item_counter.item);

        self.item_produced_event.notify_all();
    }

    #[inline]
    fn consume_start(&mut self) -> usize {
        let _lock = self.mutex.lock().unwrap();
        self.consumer_count += 1;
        self.index
    }

    #[inline]
    fn shutdown(&mut self) {
        let _lock = self
            .recycled_event
            .wait_while(self.mutex.lock().unwrap(), |_b| self.empty())
            .unwrap();

        match self.index {
            _ if self.index == SIZE => {
                self.index = SIZE-2;
                self.data[SIZE-1] = <Self as RecyclerBuffer>::NULL_ITEM
            }, 
            0 => {
                self.index = SIZE;
                self.data[0] = <Self as RecyclerBuffer>::NULL_ITEM
            }
            _ => {
                self.data[self.index] = <Self as RecyclerBuffer>::NULL_ITEM;
                self.index -= 1;
            }
        };

        self.item_produced_event.notify_all();
    }

    #[inline]
    fn drop_consumer(&mut self) {
        self.consumer_count -= 1;
    }

}

/// a reference to a managed T instance that allows mutation.
/// When the reference is destroyed then the managed T instance will
/// be recycled back into the [Recycler]. This reference is not thread safe
/// but shareable thread safe references can be created by calling [RecycleRef::to_shared]
pub struct RecycleRef<B>
where
    B: StackBuffer,
{
    buf_ptr: GuardedBufferPtr<B>,
    item_ptr: *mut ItemCounter<B::ItemType>,
}

impl<B> RecycleRef<B>
where
    B: StackBuffer + Send,
    B::ItemType: Send + Sync,
{
    pub fn new(
        buf_ptr: GuardedBufferPtr<B>,
        item_ptr: NotNullItem<<B as RecyclerBuffer>::ItemType>,
    ) -> Self {
        Self {
            buf_ptr,
            item_ptr: item_ptr.as_ptr(),
        }
    }
    /// consume this ref into a shareable form
    pub fn to_shared(mut self) -> SharedRecycleRef<B> {
        let ptr = std::mem::replace(
            &mut self.item_ptr,
            std::ptr::null::<ItemCounter<B::ItemType>>() as *mut ItemCounter<B::ItemType>,
        );
        SharedRecycleRef::new(self.buf_ptr.clone(), NonNull::new(ptr).unwrap())
    }
}

impl<B> Deref for RecycleRef<B>
where
    B: StackBuffer,
{
    type Target = <B as RecyclerBuffer>::ItemType;

    #[inline]
    fn deref(&self) -> &Self::Target {
        // item ptr is only null after consumed by to_shared
        unsafe { &self.item_ptr.as_ref().unwrap().item }
    }
}

impl<B> DerefMut for RecycleRef<B>
where
    B: StackBuffer,
{
    #[inline]
    fn deref_mut(&mut self) -> &mut Self::Target {
        unsafe { &mut self.item_ptr.as_mut().unwrap().item }
    }
}

impl<B> Drop for RecycleRef<B>
where
    B: StackBuffer,
{
    fn drop(&mut self) {
        if !self.item_ptr.is_null() {
            // we are the last reference remaining. We are now responsible for returning the
            // data to the main buffer
            self.buf_ptr
                .get()
                .recycle(NonNull::new(self.item_ptr).unwrap());
        }
    }
}

pub trait RecyclerRef<T>: Clone + Deref<Target = T> {}

/// A thread safe recycle reference that allows read access to the underlying
/// item. Once all instances of the shared recycle reference that contain the same
/// item are drop then the item is recycled back into the recycler buffer.
#[derive(Debug)]
pub struct SharedRecycleRef<B>
where
    B: StackBuffer + Send,
    <B as RecyclerBuffer>::ItemType: Send + Sync,
{
    buf_ptr: GuardedBufferPtr<B>,
    item_ptr: NotNullItem<<B as RecyclerBuffer>::ItemType>,
}

impl<B> SharedRecycleRef<B>
where
    B: StackBuffer + Send,
    <B as RecyclerBuffer>::ItemType: Send + Sync,
{
    pub fn new(
        buf_ptr: GuardedBufferPtr<B>,
        item_ptr: NotNullItem<<B as RecyclerBuffer>::ItemType>,
    ) -> Self {
        unsafe { item_ptr.as_ref() }
            .ref_counter
            .store(1, Ordering::Relaxed);
        Self { buf_ptr, item_ptr }
    }
}

// impl<B> RecyclerRef<B> for SharedRecycleRef<B> where B: RecyclerBuffer, <B as RecyclerBuffer>::ItemType: Send + Sync {}

impl<B> Deref for SharedRecycleRef<B>
where
    B: StackBuffer + Send,
    <B as RecyclerBuffer>::ItemType: Send + Sync,
{
    type Target = <B as RecyclerBuffer>::ItemType;

    #[inline]
    fn deref(&self) -> &Self::Target {
        unsafe { &self.item_ptr.as_ref().item }
    }
}

impl<B> Clone for SharedRecycleRef<B>
where
    B: StackBuffer + Send,
    <B as RecyclerBuffer>::ItemType: Send + Sync,
{
    #[inline]
    fn clone(&self) -> Self {
        let old_rc = unsafe { self.item_ptr.as_ref().increment() };

        if old_rc >= isize::MAX as u32 {
            std::process::abort();
        }

        Self {
            buf_ptr: self.buf_ptr.clone(),
            item_ptr: self.item_ptr,
        }
    }
}

impl<B> Drop for SharedRecycleRef<B>
where
    B: StackBuffer + Send,
    <B as RecyclerBuffer>::ItemType: Send + Sync,
{
    #[inline]
    fn drop(&mut self) {
        let ptr = self.item_ptr;
        let value = unsafe { ptr.as_ref() }.decrement();

        if value != 1 {
            return;
        }
        // previous value was 1 one and after decrementing the counter we are now at zero
        // we are the last reference remaining. We are now responsible for returning the
        // data to the recycler
        self.buf_ptr.get().recycle(ptr);
    }
}

unsafe impl<B> Send for SharedRecycleRef<B>
where
    B: StackBuffer + Send,
    <B as RecyclerBuffer>::ItemType: Send + Sync,
{
}
unsafe impl<B> Sync for SharedRecycleRef<B>
where
    B: StackBuffer + Send,
    <B as RecyclerBuffer>::ItemType: Send + Sync,
{
}
