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
        self.ref_counter.fetch_sub(1, Ordering::Release)
    }
}

pub trait RecyclerBuffer {
    type ItemType;
    const NULL_ITEM: *const ItemCounter<Self::ItemType> = std::ptr::null();
    fn available(&self) -> usize;
    fn empty(&self) -> bool;
    fn capacity(&self) -> usize;
    fn recycle(&mut self, item: NotNullItem<Self::ItemType>);
}

pub trait StackBuffer: RecyclerBuffer {
    fn get_one(&mut self) -> Option<ContainerType<Self::ItemType>>;
    fn wait_for_one(&mut self) -> ContainerType<Self::ItemType>;
    fn timed_wait_for_one(&mut self, wait_time: Duration) -> Option<ContainerType<Self::ItemType>>;
}

pub trait ProducerConsumerBuffer: RecyclerBuffer {
    fn consumer_last_index(&self) -> usize;
    fn consume_at(&self, index: &mut usize) -> ContainerType<Self::ItemType>;
    fn broadcast<F>(&mut self, f: F)
    where
        F: FnOnce(&mut Self::ItemType);
    fn shutdown(&mut self);
    fn increment_consumer_count(&mut self) -> usize;
    fn drop_consumer(&mut self);
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
}

#[derive(Debug)]
pub struct StaticBuffer<T, const SIZE: usize>
where
    T: Send + Sync,
{
    data: [ContainerType<T>; SIZE],
    index: usize,
    consumer_index: usize,
    consumer_count: u32,
    pub mutex: Mutex<()>,
    pub recycled_event: Condvar,
    pub item_produced_event: Condvar,
    consumer_data: Vec<ContainerType<T>>,
}

unsafe impl<T, const SIZE: usize> Send for StaticBuffer<T, SIZE> where T: Send + Sync {}

impl<T, const SIZE: usize> StaticBuffer<T, SIZE>
where
    T: Send + Sync,
{
    pub fn new(data: [ContainerType<T>; SIZE]) -> Self {
        let consumer_data =
            vec![<Self as RecyclerBuffer>::NULL_ITEM; data.len() + 1];

        Self {
            data,
            index: 0,
            consumer_data,
            consumer_index: 0,
            consumer_count: 0,
            mutex: Mutex::new(()),
            recycled_event: Condvar::new(),
            item_produced_event: Condvar::new(),
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
        self.capacity() - self.index
    }

    #[inline]
    fn empty(&self) -> bool {
        self.index == SIZE
    }

    #[inline]
    fn capacity(&self) -> usize {
        SIZE
    }
    #[inline]
    fn recycle(&mut self, item: NotNullItem<Self::ItemType>) {
        let _lock = self.mutex.lock();

        self.index -= 1;
        //*unsafe {self.data.get_unchecked_mut(self.index) } = item
        //println!("recycling into index {}", self.index);
        self.data[self.index] = item.as_ptr();
        self.recycled_event.notify_one();
    }
}

impl<T, const SIZE: usize> ProducerConsumerBuffer for StaticBuffer<T, SIZE>
where
    T: Send + Sync,
{
    fn consume_at(&self, index: &mut usize) -> ContainerType<Self::ItemType> {
        //println!("reading index {}, last is {}", index, self.consumer_last_index());
        let lock = self
            .item_produced_event
            .wait_while(self.mutex.lock().unwrap(), |_e| {
                *index == self.consumer_index
            })
            .unwrap();

        let item = self.consumer_data[*index];
        drop(lock);

        if *index == self.capacity() {
            *index = 0;
        } else {
            *index += 1;
        }
        item
    }

    #[inline]
    fn consumer_last_index(&self) -> usize {
        self.consumer_index
    }

    #[inline]
    fn broadcast<F>(&mut self, f: F)
    where
        F: FnOnce(&mut Self::ItemType),
    {
        //println!("writing to index {}, consumer index {}", self.index, self.consumer_index);
        // while empty wait until one is available
        let _lock = self
            .recycled_event
            .wait_while(self.mutex.lock().unwrap(), |_b| self.empty())
            .unwrap();

        let ptr = self.data[self.index];
        self.index += 1;

        let last_index = self.consumer_last_index();
        self.consumer_data[last_index] = ptr;

        self.consumer_index += 1;

        if self.consumer_index >= self.consumer_data.len() {
            self.consumer_index = 0;
        }

        let item_counter = unsafe { ptr.cast_mut().as_mut().unwrap() };

        item_counter
            .ref_counter
            .store(self.consumer_count, Ordering::Relaxed);

        f(&mut item_counter.item);

        self.item_produced_event.notify_all();
    }

    #[inline]
    fn shutdown(&mut self) {
        let _lock = self.mutex.lock();

        let last_index = self.consumer_last_index();
        self.consumer_data[last_index] = Self::NULL_ITEM as ContainerType<Self::ItemType>;

        self.consumer_index += 1;

        if self.consumer_index >= self.consumer_data.len() {
            self.consumer_index = 0;
        }
        self.item_produced_event.notify_all();
    }

    #[inline]
    fn drop_consumer(&mut self) {
        self.consumer_count -= 1;
    }

    #[inline]
    fn increment_consumer_count(&mut self) -> usize {
        let _lock = self.mutex.lock();
        self.consumer_count += 1;
        self.consumer_last_index()
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
