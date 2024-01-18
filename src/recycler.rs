use std::{sync::atomic::{AtomicU32, Ordering}, ptr::NonNull, time::Duration, ops::{Deref, DerefMut}};

use crate::{ContainerType, ItemCounter, DynamicBuffer, DynamicBufferPtr, NotNullItem, GuardedBufferPtr, consumer::Consumer};


pub trait RecyclerBuffer {
    
    type ItemType;
    const NULL_ITEM: * const ItemCounter<Self::ItemType> = std::ptr::null();

    fn recycle(&mut self, item: NotNullItem<Self::ItemType>);
    fn pop(&mut self) -> ContainerType<Self::ItemType>;
    fn available(&self) -> usize;
    fn empty(&self) -> bool;
    fn capacity(&self) -> usize;
    fn consumer_last_index(&self) -> usize;
    fn consume_at(&self, index: usize) -> ContainerType<Self::ItemType>;
    fn broadcast(&mut self) -> ContainerType<Self::ItemType>;
    fn shutdown(&mut self);
    fn increment_consumer_count(&mut self);
    fn drop_consumer(&mut self);
}


/// A buffer that manages the recycling of dropped items that were pulled from this buffer.
pub struct Recycler<B>
where
    B: RecyclerBuffer,
{
    inner: GuardedBufferPtr<B>,
}

// enable share behavior if the buffer is +Send and the items in the buffer are +Send
impl<B> Recycler<B>
where
    B: RecyclerBuffer + Send, <B as RecyclerBuffer>::ItemType: Send + Sync
{
    pub fn new(buf: GuardedBufferPtr<B>) -> Recycler<B> {
        Recycler { inner: buf }
    }
    /// waits for an item to be available, runs the given function, and returns
    /// a shareable reference. This avoids an intermediate [RecycleRef] creation
    #[inline]
    pub fn wait_and_share<F>(&mut self, f: F) -> SharedRecycleRef<B>
    where
        <B as RecyclerBuffer>::ItemType: Send + Sync,
        F: FnOnce(&mut <B as RecyclerBuffer>::ItemType),
    {
        // while empty wait until one is available
        let mut locked_buffer = self
            .inner
            .recycled_event
            .wait_while(self.inner.buffer.lock().unwrap(), |b| b.empty())
            .unwrap();
        let mut ptr = NotNullItem::new(locked_buffer.pop()).unwrap();

        f(unsafe { &mut ptr.as_mut().item });

        SharedRecycleRef::new(self.inner.clone(), ptr)
    }

    #[inline]
    pub fn wait_and_broadcast<F>(&mut self, f: F)
    where
        <B as RecyclerBuffer>::ItemType: Send + Sync,
        F: FnOnce(&mut <B as RecyclerBuffer>::ItemType),
    {
        // while empty wait until one is available
        let mut locked_buffer = self
            .inner
            .recycled_event
            .wait_while(self.inner.buffer.lock().unwrap(), |b| b.empty())
            .unwrap();
        let mut ptr =  NotNullItem::new(locked_buffer.broadcast()).unwrap();

        f(unsafe { &mut ptr.as_mut().item });

        self.inner.item_produced_event.notify_all();
    }

    pub fn shutdown(&self) {
        self.inner.buffer.lock().unwrap().shutdown();
        self.inner.item_produced_event.notify_all();
    }

    pub fn create_consumer(&mut self) -> Consumer<B> {
        let mut locked_buffer = self.inner.buffer.lock().unwrap();
        locked_buffer.increment_consumer_count();

        Consumer::new(
            self.inner.clone(),
            locked_buffer.consumer_last_index(),
        )
    }
}

impl<B> Recycler<B>
where
    B: RecyclerBuffer,
{
    #[inline]
    fn make_ref(&self, item_ptr: NotNullItem<<B as RecyclerBuffer>::ItemType>) -> RecycleRef<B> {
        RecycleRef {
            buf_ptr: self.inner.clone(),
            item_ptr: item_ptr.as_ptr(),
        }
    }

    pub fn take(&mut self) -> Option<RecycleRef<B>> {
        let mut inner = self.inner.buffer.lock().unwrap();
        if inner.empty() {
            // empty
            None
        } else {
            Some(self.make_ref(NonNull::new(inner.pop()).unwrap()))
        }
    }

    /// waits for one item to be available in the buffer.
    pub fn wait_and_take(&self) -> RecycleRef<B> {
        // loop while buffer is empty until at least one item becomes available
        let mut locked_buffer = self
            .inner
            .recycled_event
            .wait_while(self.inner.buffer.lock().unwrap(), |b| b.empty())
            .unwrap();

        let ptr = locked_buffer.pop();
        drop(locked_buffer);

        self.make_ref(NonNull::new(ptr).unwrap())
    }

    pub fn wait_for(&self, dur: Duration) -> Option<RecycleRef<B>> {
        // loop until one is available
        if let Ok((mut locked_buffer, timeout)) = self.inner.recycled_event.wait_timeout_while(
            self.inner.buffer.lock().unwrap(),
            dur,
            |e| e.empty(),
        ) {
            if !timeout.timed_out() {
                // we didn't time out so at least one item is available
                let ptr = locked_buffer.pop();

                // we don't need the lock any more so release it
                drop(locked_buffer);

                return Some( self.make_ref(NonNull::new(ptr).unwrap()));
            }
        }
        None
    }

    /// returns the number of items currently available in the recycler. This
    /// number can change any time after a call to this function in multithreaded scenarios.
    #[inline]
    pub fn available(&self) -> usize {
        self.inner.buffer.lock().unwrap().available()
    }

    /// returns the maximum number of items that can be stored in this recycler
    #[inline]
    pub fn capacity(&self) -> usize {
        self.inner.buffer.lock().unwrap().capacity()
    }
}


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

/// Uses the builder pattern to pre allocate T instances
/// and build the [Recycler]
pub struct RecyclerBuilder<T>
where
    T: Send,
{
    contents: Vec<ContainerType<T>>,
}


impl<T> RecyclerBuilder<T>
where
    T: Send + Sync,
{
    pub const fn new() -> Self {
        Self { contents: vec![] }
    }

    /// moves a pre-constructed instance of T and makes it available in the recycler buffer when created
    pub fn push(mut self, item: T) -> Self {
        self.contents.push(make_container(item));
        Self {
            contents: self.contents,
        }
    }

    pub fn push_all<I>(mut self, items: I) -> Self
    where
        I: IntoIterator<Item = T>,
    {
        self.contents
            .extend(items.into_iter().map(|item| make_container(item)));
        Self {
            contents: self.contents,
        }
    }

    pub fn generate<GenFn>(self, count: usize, generator: GenFn) -> Self
    where
        GenFn: FnMut(usize) -> T,
    {
        self.push_all((0..count).map(generator))
    }

    /// creates the recycler
    pub fn build(self) -> Recycler<DynamicBuffer<T>> {
        let contents = self.contents.into_boxed_slice();

        Recycler::new(DynamicBufferPtr::new(crate::BufferControl::new(
            DynamicBuffer::new(contents),
        )))
    }
}


/// a reference to a managed T instance that allows mutation.
/// When the reference is destroyed then the managed T instance will
/// be recycled back into the [Recycler]. This reference is not thread safe
/// but shareable thread safe references can be created by calling [RecycleRef::to_shared]
pub struct RecycleRef<B>
where
    B: RecyclerBuffer,
{
    buf_ptr: GuardedBufferPtr<B>,
    item_ptr: *mut ItemCounter<B::ItemType>,
}

impl<B> RecycleRef<B>
where
    B: RecyclerBuffer + Send,
    B::ItemType: Send + Sync,
{
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
    B: RecyclerBuffer,
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
    B: RecyclerBuffer,
{
    #[inline]
    fn deref_mut(&mut self) -> &mut Self::Target {
        unsafe { &mut self.item_ptr.as_mut().unwrap().item }
    }
}

impl<B> Drop for RecycleRef<B>
where
    B: RecyclerBuffer,
{
    fn drop(&mut self) {
        if !self.item_ptr.is_null() {
            // we are the last reference remaining. We are now responsible for returning the
            // data to the main buffer
            self.buf_ptr
                .buffer
                .lock()
                .unwrap()
                .recycle(NonNull::new(self.item_ptr).unwrap());
            self.buf_ptr.recycled_event.notify_one();
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
    B: RecyclerBuffer + Send,
    <B as RecyclerBuffer>::ItemType: Send + Sync,
{
    buf_ptr: GuardedBufferPtr<B>,
    item_ptr: NotNullItem<<B as RecyclerBuffer>::ItemType>,
}

impl<B> SharedRecycleRef<B>
where
    B: RecyclerBuffer + Send,
    <B as RecyclerBuffer>::ItemType: Send + Sync,
{
    fn new(
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
    B: RecyclerBuffer + Send,
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
    B: RecyclerBuffer + Send,
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
    B: RecyclerBuffer + Send,
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
        self.buf_ptr.buffer.lock().unwrap().recycle(ptr);
        self.buf_ptr.recycled_event.notify_one();
    }
}

unsafe impl<B> Send for SharedRecycleRef<B>
where
    B: RecyclerBuffer + Send,
    <B as RecyclerBuffer>::ItemType: Send + Sync,
{
}
unsafe impl<B> Sync for SharedRecycleRef<B>
where
    B: RecyclerBuffer + Send,
    <B as RecyclerBuffer>::ItemType: Send + Sync,
{
}
