use std::{
    ops::{Deref, DerefMut},
    ptr::NonNull,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Condvar, Mutex,
    },
    time::Duration,
};

use crate::sync_cell::SyncUnsafeCell;

pub type NotNullItem<T> = NonNull<ItemCounter<T>>;
pub type ContainerTypePtr<T> = *const ItemCounter<T>;
pub type ContainerType<T> = ItemCounter<T>;
pub type GuardedBufferPtr<B> = Arc<SyncUnsafeCell<B>>;

pub type DynamicBufferPtr<T> = GuardedBufferPtr<DynamicBuffer<T>>;
pub type StaticBufferPtr<T, const N: usize> = GuardedBufferPtr<StaticBuffer<T, N>>;

pub fn make_container_ptr<T>(item: T) -> ContainerTypePtr<T> {
    // move the item to the heap via box
    let boxed = ItemCounter {
        item,
        ref_counter: AtomicUsize::new(0),
        state: Mutex::new(ItemState{
            free: true,
            sequence: 0,
            terminator: false
        }),
        item_recycle: Condvar::new(),
        item_ready: Condvar::new(),
    }
    .into_box();
    // get a pointer from the heap
    Box::into_raw(boxed)
}

pub fn make_container<T>(item: T) -> ContainerType<T> {
    // move the item to the heap via box
    ItemCounter {
        item,
        ref_counter: AtomicUsize::new(0),
        state: Mutex::new(ItemState{
            free: true,
            sequence: 0,
            terminator: false
        }),
        item_recycle: Condvar::new(),
        item_ready: Condvar::new(),
    }
}

#[derive(Debug)]
struct ItemState {
    sequence: usize,
    free: bool,
    terminator: bool
}

impl ItemState {

    #[inline]
    fn publish(&mut self, sequence: usize) {
        self.free = false;
        self.sequence = sequence;
    }

    fn set_termination_sequnece(&mut self, sequence: usize) {
        self.free = false;
        self.sequence = sequence;
        self.terminator = true;
    }
}
#[derive(Debug)]
pub struct ItemCounter<T> {
    pub item: T,
    ref_counter: AtomicUsize,
    state: Mutex<ItemState>,
    item_recycle: Condvar,
    item_ready: Condvar,
}

unsafe impl<T> Send for ItemCounter<T> where T: Send {}
unsafe impl<T> Sync for ItemCounter<T> where T: Sync {}

impl<T> ItemCounter<T> {
    #[inline]
    fn into_box(self) -> Box<Self> {
        Box::new(self)
    }

    #[inline]
    pub fn increment(&self) -> usize {
        self.ref_counter.fetch_add(1, Ordering::Relaxed)
    }

    #[inline]
    pub fn decrement(&self) -> usize {
        self.ref_counter.fetch_sub(1, Ordering::Relaxed)
    }

    #[inline]
    pub(crate) fn recycle(&mut self) {
        self.state.lock().unwrap().free = true;
        self.item_recycle.notify_all();
    }

    #[inline]
    pub(crate) fn wait_until_ready(&self, consumer_counter: usize) -> bool {
        let state_lock = self
            .item_ready
            .wait_while(self.state.lock().unwrap(), |s| {
                s.sequence < consumer_counter
            })
            .unwrap();

        state_lock.terminator
    }

    #[inline]
    pub(crate) fn wait_until_free(&mut self, prod_sequnce: usize, ref_count: usize) {
        let mut state_lock = self
            .item_recycle
            .wait_while(self.state.lock().unwrap(), |s| !s.free)
            .unwrap();

        state_lock.publish(prod_sequnce);
        self.ref_counter.store(ref_count, Ordering::Relaxed);
        drop(state_lock);
    }

    #[inline]
    pub(crate) fn producer_take<F>(&mut self, prod_sequnce: usize, ref_count: usize, f: F)
    where
        F: FnOnce(&mut T),
    {
        let mut state_lock = self
            .item_recycle
            .wait_while(self.state.lock().unwrap(), |s| !s.free)
            .unwrap();

        // assert!(*lock == Some(prod_sequnce));
        self.ref_counter.store(ref_count, Ordering::Relaxed);
        f(&mut self.item);
        state_lock.publish(prod_sequnce);
        drop(state_lock);
        
        self.item_ready.notify_all();
    }

    pub(crate) fn mark_shutdown(&mut self, prod_sequnce: usize, ref_cnt: usize) {
        let mut state_lock = self
            .item_recycle
            .wait_while(self.state.lock().unwrap(), |s| !s.free)
            .unwrap();
        state_lock.set_termination_sequnece(prod_sequnce);
        self.ref_counter.store(ref_cnt, Ordering::Relaxed);
        drop(state_lock);

        self.item_ready.notify_all();
    }
}

pub trait RecyclerBuffer {
    type ItemType;
    const NULL_ITEM: *const ItemCounter<Self::ItemType> = std::ptr::null();
    fn capacity(&self) -> usize;
}

pub trait StackBuffer: RecyclerBuffer {
    fn get_one(&mut self) -> Option<ContainerTypePtr<Self::ItemType>>;
    fn wait_for_one(&mut self) -> ContainerTypePtr<Self::ItemType>;
    fn timed_wait_for_one(
        &mut self,
        wait_time: Duration,
    ) -> Option<ContainerTypePtr<Self::ItemType>>;
    fn recycle(&mut self, item: NotNullItem<Self::ItemType>);
    fn available(&self) -> u32;
    fn empty(&self) -> bool;
}

pub trait ProducerConsumerBuffer: RecyclerBuffer {
    const MAX: usize;

    fn wait_for_one(&mut self) -> &mut ContainerType<Self::ItemType>;

    //fn consume_at(&mut self, consumer_state: &mut ConsumerState) -> (ContainerType<Self::ItemType>, usize);
    fn produce_item<F>(&mut self, f: F)
    where
        F: FnOnce(&mut Self::ItemType);
    fn shutdown(&mut self);
    fn drop_consumer(&mut self);
    fn add_consumer(&mut self);
    fn consume_next(
        &mut self,
        consumer_counter: usize,
    ) -> Option<&mut ContainerType<Self::ItemType>>;
}

#[derive(Debug)]
pub struct DynamicBuffer<T> {
    data: Box<[ContainerTypePtr<T>]>,
    index: usize,
    pub mutex: Mutex<()>,
    pub recycled_event: Condvar,
    pub readied: Condvar,
}

impl<T> DynamicBuffer<T> {
    pub fn new(data: Box<[ContainerTypePtr<T>]>) -> Self {
        Self {
            data,
            index: 0,
            mutex: Mutex::new(()),
            recycled_event: Condvar::new(),
            readied: Condvar::new(),
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
    fn capacity(&self) -> usize {
        self.data.len()
    }
}

impl<T> StackBuffer for DynamicBuffer<T> {
    #[inline]
    /// takes an item from the internal buffer
    fn wait_for_one(&mut self) -> ContainerTypePtr<T> {
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

    fn timed_wait_for_one(
        &mut self,
        wait_time: Duration,
    ) -> Option<ContainerTypePtr<Self::ItemType>> {
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

    fn get_one(&mut self) -> Option<ContainerTypePtr<Self::ItemType>> {
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
    consumer_count: u32,
    producer_counter: usize,
}

unsafe impl<T, const SIZE: usize> Send for StaticBuffer<T, SIZE> where T: Send + Sync {}

impl<T, const SIZE: usize> StaticBuffer<T, SIZE>
where
    T: Send + Sync,
{
    pub fn new(data: [ContainerType<T>; SIZE]) -> Self {
        for item in &data {
            item.ref_counter.store(0, Ordering::Relaxed);
        }
        Self {
            data,
            consumer_count: 0,
            producer_counter: 0,
        }
    }

    #[inline]
    fn producer_next_item(&mut self) ->(&mut ContainerType<T>, usize, u32) { 
        self.producer_counter += 1;
        let current_producer_counter = self.producer_counter;
        (&mut self.data[current_producer_counter % self.capacity()], current_producer_counter, self.consumer_count)
    }
}

impl<T, const SIZE: usize> RecyclerBuffer for StaticBuffer<T, SIZE>
where
    T: Send + Sync,
{
    type ItemType = T;

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


    fn wait_for_one(&mut self) -> &mut ContainerType<Self::ItemType> {
        
        let (next_item, producer_counter, consumer_count) = self.producer_next_item();
        next_item.wait_until_free(producer_counter, consumer_count as usize);
        next_item
    }

    #[inline]
    fn produce_item<F>(&mut self, f: F)
    where
        F: FnOnce(&mut Self::ItemType),
    {
        let (next_item, producer_counter, consumer_count) = self.producer_next_item();

        next_item.producer_take(producer_counter, consumer_count as usize, f);

    }

    #[inline]
    fn add_consumer(&mut self) {
        self.consumer_count += 1;
    }

    #[inline]
    fn consume_next(
        &mut self,
        consumer_counter: usize,
    ) -> Option<&mut ContainerType<Self::ItemType>> {
        let item = &mut self.data[consumer_counter % self.capacity() as usize];

        if item.wait_until_ready(consumer_counter) {
            // println!("quitting this consumer");
            None
        } else {
            Some(item)
        }
    }

    #[inline]
    fn shutdown(&mut self) {
        let (next_item, producer_counter, consumer_count) = self.producer_next_item();

        next_item.mark_shutdown(producer_counter, consumer_count as usize);


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
        unsafe {
            self.item_ptr.as_ref().increment();
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
