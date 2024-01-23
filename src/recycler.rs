use std::{ptr::NonNull, time::Duration};

use crate::{
    buffer::{
        make_container, ContainerType, DynamicBuffer, DynamicBufferPtr, GuardedBufferPtr,
        NotNullItem, ProducerConsumerBuffer, RecycleRef, RecyclerBuffer, SharedRecycleRef,
        StackBuffer,
    },
    consumer::Consumer,
    sync_cell::SyncUnsafeCell,
};

/// A buffer that manages the recycling of dropped items that were pulled from this buffer.
pub struct Recycler<B>
where
    B: RecyclerBuffer,
    <B as RecyclerBuffer>::ItemType: Send + Sync,
{
    inner: GuardedBufferPtr<B>,
}

// enable share behavior if the buffer is +Send and the items in the buffer are +Send
impl<B> Recycler<B>
where
    B: RecyclerBuffer + Send,
    <B as RecyclerBuffer>::ItemType: Send + Sync,
{
    pub fn new(buf: GuardedBufferPtr<B>) -> Self {
        Self { inner: buf }
    }

    /// returns the maximum number of items that can be stored in this recycler
    #[inline]
    pub fn capacity(&self) -> usize {
        self.inner.get().capacity() as usize
    }


}

// recycler capabilities for when the underlying buffer implements StackBuffer
impl<B> Recycler<B>
where
    B: StackBuffer + Send,
    <B as RecyclerBuffer>::ItemType: Send + Sync,
{
    /// waits for an item to be available, runs the given function, and returns
    /// a shareable reference. This avoids an intermediate [RecycleRef] creation
    #[inline]
    pub fn wait_and_share<F>(&mut self, f: F) -> SharedRecycleRef<B>
    where
        <B as RecyclerBuffer>::ItemType: Send + Sync,
        F: FnOnce(&mut <B as RecyclerBuffer>::ItemType),
    {
        let mut ptr = NotNullItem::new(self.inner.get().wait_for_one().cast_mut()).unwrap();

        f(unsafe { &mut ptr.as_mut().item });

        SharedRecycleRef::new(self.inner.clone(), ptr)
    }

    /// returns the number of items currently available in the recycler. This
    /// number can change any time after a call to this function in multithreaded scenarios.
    #[inline]
    pub fn available(&self) -> usize {
        self.inner.get().available() as usize
    }

    pub fn take(&mut self) -> Option<RecycleRef<B>> {
        self.inner
            .get()
            .get_one()
            .map(|i| self.make_ref(NonNull::new(i.cast_mut()).unwrap()))
    }

    /// waits for one item to be available in the buffer.
    pub fn wait_and_take(&self) -> RecycleRef<B> {
        let ptr = self.inner.get().wait_for_one();

        self.make_ref(NonNull::new(ptr.cast_mut()).unwrap())
    }
    pub fn wait_for(&self, dur: Duration) -> Option<RecycleRef<B>> {
        // loop until one is available
        self.inner
            .get()
            .timed_wait_for_one(dur)
            .and_then(|item| Some(self.make_ref(NonNull::new(item.cast_mut()).unwrap())))
    }

    #[inline]
    fn make_ref(&self, item_ptr: NotNullItem<<B as RecyclerBuffer>::ItemType>) -> RecycleRef<B> {
        RecycleRef::new(self.inner.clone(), item_ptr)
    }
}

impl<B> Recycler<B>
where
    B: ProducerConsumerBuffer + Send,
    <B as RecyclerBuffer>::ItemType: Send + Sync,
{
    #[inline]
    pub fn wait_and_broadcast<F>(&mut self, f: F)
    where
        <B as RecyclerBuffer>::ItemType: Send + Sync,
        F: FnOnce(&mut <B as RecyclerBuffer>::ItemType),
    {
        self.inner.get().broadcast(f);
    }

    pub fn create_consumer(&mut self) -> Consumer<B> {
        let mut new_consumer = Consumer::new(
            self.inner.clone(),
        );
        self.inner.get().add_consumer(&mut new_consumer);

        new_consumer

    }


    pub fn shutdown(&self) {
        self.inner.get().shutdown();
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

        Recycler::new(DynamicBufferPtr::new(SyncUnsafeCell::new(
            DynamicBuffer::new(contents),
        )))
    }
}
