use std::{sync::atomic::{AtomicU32, Ordering}, ptr::NonNull, time::Duration, ops::{Deref, DerefMut}};

use crate::{consumer::Consumer, buffer::{RecyclerBuffer, NotNullItem, ContainerType, RecycleRef, make_container, BufferControl, SharedRecycleRef, GuardedBufferPtr, DynamicBufferPtr, DynamicBuffer}};


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
    B: RecyclerBuffer + Send, <B as RecyclerBuffer>::ItemType: Send + Sync
{
    #[inline]
    fn make_ref(&self, item_ptr: NotNullItem<<B as RecyclerBuffer>::ItemType>) -> RecycleRef<B> {
        RecycleRef::new(
            self.inner.clone(),
            item_ptr,
        )
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

        Recycler::new(DynamicBufferPtr::new(BufferControl::new(
            DynamicBuffer::new(contents),
        )))
    }
}
