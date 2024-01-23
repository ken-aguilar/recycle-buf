use std::{
    ops::{Deref, DerefMut},
    ptr::NonNull,
    sync::{
        atomic::{AtomicU32, Ordering, AtomicUsize},
        Arc, Condvar, Mutex,
    },
    time::Duration,
};


use crate::{sync_cell::SyncUnsafeCell, consumer::Consumer};

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

    #[inline]
    pub(crate) fn is_free(&self) -> bool{
        self.ref_counter.load(Ordering::Relaxed) == 0
    }

    #[inline]
    pub(crate) fn is_in_use(&self) -> bool{
        self.ref_counter.load(Ordering::Relaxed) > 0
    }

}

pub trait RecyclerBuffer {
    type ItemType;
    const NULL_ITEM: *const ItemCounter<Self::ItemType> = std::ptr::null();
    fn capacity(&self) -> u32;
}

pub trait StackBuffer: RecyclerBuffer {
    fn get_one(&mut self) -> Option<ContainerType<Self::ItemType>>;
    fn wait_for_one(&mut self) -> ContainerType<Self::ItemType>;
    fn timed_wait_for_one(&mut self, wait_time: Duration) -> Option<ContainerType<Self::ItemType>>;
    fn recycle(&mut self, item: NotNullItem<Self::ItemType>);
    fn available(&self) -> u32;
    fn empty(&self) -> bool;
}

pub(crate) trait ProducerConsumerBuffer: RecyclerBuffer {
    const MAX: usize;

    //fn consume_at(&mut self, consumer_state: &mut ConsumerState) -> (ContainerType<Self::ItemType>, usize);
    fn broadcast<F>(&mut self, f: F)
    where
        F: FnOnce(&mut Self::ItemType);
    fn shutdown(&mut self);
    fn drop_consumer(&mut self);
    fn recycle(&mut self);
    fn add_consumer<B>(&mut self, consumer: &mut Consumer<B>)  where B: ProducerConsumerBuffer + Send, <B as RecyclerBuffer>::ItemType: Send + Sync;
    fn consume_next(&self, consumer_counter: usize) -> ContainerType<Self::ItemType>;

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
    fn capacity(&self) -> u32 {
        self.data.len() as u32
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

    #[inline]
    fn available(&self) -> u32 {
        (self.data.len() - self.index) as u32
    }

    #[inline]
    fn empty(&self) -> bool {
        self.index == self.data.len()
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
//    buffer_state: BufferState,
    consumer_count: u32,
    mutex: Mutex<()>,
    recycled_event: Condvar,
    item_produced_event: Condvar,
    pub(crate) producer_counter: usize,
}

unsafe impl<T, const SIZE: usize> Send for StaticBuffer<T, SIZE> where T: Send + Sync {}

impl<T, const SIZE: usize> StaticBuffer<T, SIZE>
where
    T: Send + Sync,
{
    pub fn new(data: [ContainerType<T>; SIZE]) -> Self {
        unsafe {
            for item in data {
                item.cast_mut().as_mut().unwrap().ref_counter.store(0, Ordering::Relaxed);
            }
        }
        Self {
            data,
            consumer_count: 0,
            mutex: Mutex::new(()),
            recycled_event: Condvar::new(),
            item_produced_event: Condvar::new(),
            producer_counter: 0
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
                if !ptr.is_null() {
                    drop(Box::from_raw(ptr.cast_mut()));
                }
            })
    }
}

impl<T, const SIZE: usize> RecyclerBuffer for StaticBuffer<T, SIZE>
where
    T: Send + Sync,
{
    type ItemType = T;

    // #[inline]
    // fn available(&self) -> u32 {
    //     self.buffer_state.available()
    // }

    // #[inline]
    // fn empty(&self) -> bool {
    //     self.buffer_state.is_empty()
    // }

    #[inline]
    fn capacity(&self) -> u32 {
        SIZE as u32
    }

}

impl<T, const SIZE: usize> ProducerConsumerBuffer for StaticBuffer<T, SIZE>
where
    T: Send + Sync,
{

    const MAX: usize = SIZE;

    #[inline]
    fn recycle(&mut self) {

        //println!("recycling {index_to_recyle}");

        self.recycled_event.notify_one();
    }

    #[inline]
    fn broadcast<F>(&mut self, f: F)
    where
        F: FnOnce(&mut Self::ItemType),
    {


        let index = self.producer_counter % self.capacity() as usize;
        
        let next_item = unsafe {self.data[index].cast_mut().as_mut().unwrap()};
        
        let _lock = if next_item.is_in_use() {
            //println!("Producer counter {}: waiting for {index}", self.producer_counter);
            // our next item is still in use, wait for a recycle event and check again
            // while empty wait until one is available
            self
            .recycled_event
            .wait_while(self.mutex.lock().unwrap(), |_b| next_item.is_in_use()).unwrap()
        } else {
            self.mutex.lock().unwrap()
        };
    
        let ref_count = self.consumer_count;

        //println!("Producer counter {}: Writing to item at {index}, ref count {ref_count}", self.producer_counter);
        next_item
            .ref_counter
            .store(ref_count, Ordering::Relaxed);

        f(&mut next_item.item);

        // assert!(next_item.ref_counter.load(Ordering::Relaxed) == ref_count);
        self.producer_counter += 1;
 
        self.item_produced_event.notify_all();
    }

    #[inline]
    fn add_consumer<B>(&mut self, consumer: &mut Consumer<B>) where B: ProducerConsumerBuffer + Send, <B as RecyclerBuffer>::ItemType: Send + Sync {
        let _lock = self.mutex.lock().unwrap();

        self.consumer_count += 1;
//        consumer.get_state().join(&self.buffer_state);

    }

    #[inline]
    fn consume_next(&self, consumer_counter: usize) -> ContainerType<Self::ItemType> {
        //let mut lock = self.mutex.lock().unwrap();

        if self.producer_counter <= consumer_counter {
            // wait until next produce event
            let _lock = self.item_produced_event.wait_while(self.mutex.lock().unwrap(), |_b| {
                self.producer_counter <= consumer_counter
            }).unwrap();
        }

        let index = consumer_counter % self.capacity() as usize;
        self.data[index]
    }

    #[inline]
    fn shutdown(&mut self) {

        let index = self.producer_counter % self.capacity() as usize;
        
        let next_item = unsafe {self.data[index].cast_mut().as_mut().unwrap()};
        
        if !next_item.is_free() {
            // our next item is still in use, wait for a recycle event and check again
            // while empty wait until one is available
            let _lock = self
            .recycled_event
            .wait_while(self.mutex.lock().unwrap(), |_b| next_item.is_free())
            .unwrap();
        }

        let raw_ptr = next_item as *mut ItemCounter<T>;

        unsafe {    
            // since we are going to override the item in the data array with null
            // we will need to drop it, otherwise the pointer will be lost
            drop(Box::from_raw(raw_ptr));
        }
        self.data[index] = <Self as RecyclerBuffer>::NULL_ITEM;

        self.producer_counter += 1;
        
        self.item_produced_event.notify_all();
    }

    #[inline]
    fn drop_consumer(&mut self) {
        let _lock = self.mutex.lock().unwrap();
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
