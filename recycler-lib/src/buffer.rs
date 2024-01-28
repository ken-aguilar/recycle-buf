use std::{
    marker::PhantomData, ops::{Deref, DerefMut}, ptr::NonNull, sync::{
        Arc, Condvar, Mutex,
    }, time::Duration
};

use crate::sync_cell::SyncUnsafeCell;
use crossbeam_utils::CachePadded;

pub type NotNullItem<T> = NonNull<ItemCounter<T>>;
pub type ContainerTypePtr<T> = *const ItemCounter<T>;
pub type ContainerType<T> = ItemCounter<T>;
pub type GuardedBufferPtr<B> = Arc<SyncUnsafeCell<B>>;

pub type DynamicBufferPtr<T> = GuardedBufferPtr<DynamicBuffer<T>>;
pub type StaticBufferPtr<T, C> = GuardedBufferPtr<SequenceBuffer<T, C>>;

pub fn make_container_ptr<T>(item: T) -> ContainerTypePtr<T> {
    // move the item to the heap via box
    let boxed = ItemCounter::new(item)
    .into_box();
    // get a pointer from the heap
    Box::into_raw(boxed)
}

#[derive(Debug)]
struct ItemState {
    sequence: CachePadded<usize>,
    ref_counter: CachePadded<usize>,
}

impl ItemState {

    #[inline]
    fn publish(&mut self, sequence: usize) {
        *self.sequence = sequence;
    }

    fn set_termination_sequnece(&mut self) {
        *self.sequence = usize::MAX;
    }
}
#[derive(Debug)]
pub struct ItemCounter<T> {
    pub item: T,
    state: Mutex<ItemState>,
    item_recycle: Condvar,
    item_ready: Condvar,
}

unsafe impl<T> Sync for ItemCounter<T> where T: Sync {}

impl<T> ItemCounter<T> {

    pub fn new(item: T) -> Self {
        Self {
            item,
            state: Mutex::new(ItemState{
                sequence: CachePadded::new(0),
                ref_counter: CachePadded::new(0)
            }),
            item_recycle: Condvar::new(),
            item_ready: Condvar::new(),
        }
    }

    #[inline]
    fn into_box(self) -> Box<Self> {
        Box::new(self)
    }

    #[inline]
    pub fn increment_ref_count(&self)  {
        let mut lock = self.state.lock().unwrap();
        
        *lock.ref_counter += 1;
    }

    #[inline]
    pub fn decrement_ref_count(&self) -> bool{
        let mut lock = self.state.lock().unwrap();
        
        debug_assert!(*lock.ref_counter != 0);

        *lock.ref_counter -= 1;
        if *lock.ref_counter == 0 {
            self.item_recycle.notify_one();
            true
        } else {
            false
        }

    }

    #[inline]
    pub(crate) fn wait_until_ready(&self, consumer_counter: usize) -> bool {
        let state_lock = self
            .item_ready
            .wait_while(self.state.lock().unwrap(), |s| {
                *s.sequence < consumer_counter
            })
            .unwrap();

        *state_lock.sequence == usize::MAX
    }

    pub(crate) fn take_one(&mut self) -> bool {
        let mut state_lock = self.state.lock().unwrap();

        if *state_lock.ref_counter == 0 {
            *state_lock.ref_counter = 1;
            true
        } else {
            false
        }
    }

    #[inline]
    pub(crate) fn wait_until_free(&mut self) {
        let mut state_lock = self
            .item_recycle
            .wait_while(self.state.lock().unwrap(), |s| *s.ref_counter != 0)
            .unwrap();

        *state_lock.ref_counter = 1;
    }

    #[inline]
    pub(crate) fn producer_take<F>(&mut self, prod_sequnce: usize, ref_count: usize, f: F)
    where
        F: FnOnce(&mut T),
    {
        let mut state_lock = self
            .item_recycle
            .wait_while(self.state.lock().unwrap(), |s| *s.ref_counter != 0)
            .unwrap();

        f(&mut self.item);
        *state_lock.ref_counter = ref_count;
        state_lock.publish(prod_sequnce);
        drop(state_lock);
        
        self.item_ready.notify_all();
    }

    pub(crate) fn mark_shutdown(&mut self, ref_cnt: usize) {
        let mut state_lock = self
            .item_recycle
            .wait_while(self.state.lock().unwrap(), |s| *s.ref_counter != 0)
            .unwrap();
        state_lock.set_termination_sequnece();
        *state_lock.ref_counter = ref_cnt;
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

    fn get_one(&mut self) -> Option<ContainerTypePtr<Self::ItemType>>;

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
        self.data[self.index] = item.as_ptr();
        self.recycled_event.notify_one();
    }
}

#[derive(Debug)]
pub struct SequenceBuffer<T, C> 
where 
    C: AsRef<[ItemCounter<T>]> + AsMut<[ItemCounter<T>]>,
    T: Send + Sync,
{
    data: C,
    consumer_count: u32,
    producer_counter: usize,
    pd: PhantomData<T>
}

impl<T, C> SequenceBuffer<T, C>
where 
    C: AsRef<[ItemCounter<T>]> + AsMut<[ItemCounter<T>]>,
    T: Send + Sync,
{
    pub fn new(data: C) -> Self {

        Self {
            data,
            consumer_count: 0,
            producer_counter: 0,
            pd: PhantomData
        }
    }

    #[inline]
    fn producer_next_item(&mut self) ->(&mut ContainerType<T>, usize, u32) { 
        let cap = self.capacity() as usize;
        self.producer_counter += 1;
        (&mut self.data.as_mut()[ self.producer_counter % cap],  self.producer_counter, self.consumer_count)
    }
}

impl<T, C> RecyclerBuffer for SequenceBuffer<T, C>
where 
    C: AsRef<[ItemCounter<T>]> + AsMut<[ItemCounter<T>]>,
    T: Send + Sync,
{
    type ItemType = T;

    #[inline]
    fn capacity(&self) -> usize {
        self.data.as_ref().len()
    }
}

impl<T, C> ProducerConsumerBuffer for SequenceBuffer<T, C>
where 
    C: AsRef<[ItemCounter<T>]> + AsMut<[ItemCounter<T>]>,
    T: Send + Sync,
{

    fn get_one(&mut self) -> Option<ContainerTypePtr<Self::ItemType>> {
        let (next_item, _producer_counter, _consumer_count) = self.producer_next_item();
        if next_item.take_one() {
            Some(next_item)
        } else {
            None
        }
    }
    fn wait_for_one(&mut self) -> &mut ContainerType<Self::ItemType> {
        // takes an item from the buffer without publishing it to consumers
        let (next_item, _producer_counter, _consumer_count) = self.producer_next_item();
        next_item.wait_until_free();
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
        let cap = self.capacity();
        let data = self.data.as_mut();

        let item = &mut data[consumer_counter % cap as usize];

        if item.wait_until_ready(consumer_counter) {
            // println!("quitting this consumer");
            None
        } else {
            Some(item)
        }
    }

    #[inline]
    fn shutdown(&mut self) {
        let (next_item, _producer_counter, consumer_count) = self.producer_next_item();

        next_item.mark_shutdown(consumer_count as usize);

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
        let item = unsafe { item_ptr.as_ref() };

        let mut lock = item.state.lock().unwrap();
        *lock.ref_counter = 1;
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
            self.item_ptr.as_ref().increment_ref_count();
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
        let item = unsafe { ptr.as_ref() };

        
        // previous value was 1 one and after decrementing the counter we are now at zero
        // we are the last reference remaining. We are now responsible for returning the
        // data to the recycler

        if item.decrement_ref_count() {
            self.buf_ptr.get().recycle(ptr);
        }
        
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


