//! This module implements a recycler buffer system that allows for pre-allocation and memory reuse
//! by leveraging the Rust language's drop mechanics. The main motivation for this system is avoiding
//! memory allocations of large and/or expensive to create objects in a realtime environment

mod consumer;

use consumer::{Consumer, ConsumerRef};

use std::{
    ops::{Deref, DerefMut},
    ptr::NonNull,
    sync::{
        atomic::{AtomicU32, Ordering},
        Arc, Condvar, Mutex, Weak,
    },
    time::Duration,
};

pub type GuardedBufferPtr<B> = Arc<BufferControl<B>>;
pub type WeakGuardedBufferPtr<B> = Weak<BufferControl<B>>;

pub type DynamicBufferPtr<T> = GuardedBufferPtr<DynamicBuffer<T>>;
pub type StaticBufferPtr<T, const N: usize> = GuardedBufferPtr<StaticBuffer<T, N>>;

pub type NotNullItem<T> = NonNull<ItemCounter<T>>;
pub type ContainerType<T> = * mut ItemCounter<T>;


#[derive(Debug)]
pub struct BufferControl<B>
where
    B: RecyclerBuffer,
{
    buffer: Mutex<B>,
    recycled_event: Condvar,
    item_produced_event: Condvar,
}

impl<B> BufferControl<B>
where
    B: RecyclerBuffer,
{
    fn new(buffer: B) -> BufferControl<B> {
        BufferControl {
            buffer: Mutex::new(buffer),
            recycled_event: Condvar::new(),
            item_produced_event: Condvar::new(),
        }
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

pub struct ItemCounter<T> {
    ref_counter: AtomicU32,
    item: T,
}

unsafe impl<T> Send for ItemCounter<T> where T: Send {}

impl<T> ItemCounter<T> {
    fn into_box(self) -> Box<Self> {
        Box::new(self)
    }

    #[inline]
    fn increment(&self) -> u32 {
        self.ref_counter.fetch_add(1, Ordering::Relaxed)
    }

    #[inline]
    fn decrement(&self) -> u32 {
        self.ref_counter.fetch_sub(1, Ordering::Release)
    }
}

fn make_container<T>(item: T) -> ContainerType<T> {
    // move the item to the heap via box
    let boxed = ItemCounter {
        item,
        ref_counter: AtomicU32::new(0),
    }
    .into_box();
    // get a pointer from the heap
    Box::into_raw(boxed)
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

/// A buffer that manages the recycling of dropped items that were pulled from this buffer.
pub struct Recycler<B>
where
    B: RecyclerBuffer,
{
    inner: GuardedBufferPtr<B>,
}

pub trait RecyclerBuffer {
    
    type ItemType;
    const NullItem: * const ItemCounter<Self::ItemType> = std::ptr::null();

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

#[derive(Debug)]
pub struct DynamicBuffer<T> {
    data: Box<[ContainerType<T>]>,
    index: usize,
    consumer_index: u32,
    consumer_data: Vec<ContainerType<T>>,
}

impl<T> DynamicBuffer<T> {
    fn new(data: Box<[ContainerType<T>]>) -> DynamicBuffer<T> {
        let consumer_data = data.to_vec();

        DynamicBuffer {
            data,
            index: 0,
            consumer_data,
            consumer_index: 0,
        }
    }
}

unsafe impl<T> Send for DynamicBuffer<T> where T: Send {}

impl<T> Drop for DynamicBuffer<T> {
    fn drop(&mut self) {
        // we have to manually drop each item in the buffer
        self.data
            .iter()
            .map(|d| unsafe { Box::from_raw(*d) })
            .for_each(|item| drop(item))
    }
}

#[derive(Debug)]
pub struct StaticBuffer<T, const SIZE: usize> {
    data: [ContainerType<T>; SIZE],
    consumer_data: Vec<ContainerType<T>>,
    index: usize,
    consumer_index: usize,
    consumer_count: u32,
}

unsafe impl<T, const SIZE: usize> Send for StaticBuffer<T, SIZE> where T: Send {}

impl<T, const SIZE: usize> StaticBuffer<T, SIZE> {
    fn new(data: [ContainerType<T>; SIZE]) -> StaticBuffer<T, SIZE> {
        let consumer_data = vec![Self::NullItem as ContainerType<T>; data.len() + 1];

        StaticBuffer {
            data,
            index: 0,
            consumer_data,
            consumer_index: 0,
            consumer_count: 0,
        }
    }

}

impl<T, const SIZE: usize> Drop for StaticBuffer<T, SIZE> {
    fn drop(&mut self) {
        // we have to manually drop each item in the buffer
        self.data.iter().for_each(|ptr: &*mut ItemCounter<T>| unsafe {
            drop(Box::from_raw(*ptr));
        })
    }
}

impl<T> RecyclerBuffer for DynamicBuffer<T> {
    type ItemType = T;

    #[inline]
    fn recycle(&mut self, item: NotNullItem<T>) {
        self.index -= 1;
        //*unsafe {self.data.get_unchecked_mut(self.index) } = item
        self.data[self.index] = item.as_ptr();
    }

    #[inline]
    /// takes an item from the internal buffer
    fn pop(&mut self) -> ContainerType<T> {
        //let item= unsafe {self.data.get_unchecked(self.index)};
        let item = self.data[self.index];
        self.index += 1;
        //*item
        item
    }

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

    fn consume_at(&self, index: usize) -> ContainerType<Self::ItemType> {
        self.consumer_data[index as usize]
    }

    fn consumer_last_index(&self) -> usize {
        self.consumer_index as usize
    }

    fn broadcast(&mut self) -> ContainerType<T> {
        let ptr = self.pop();
        let last_index = self.consumer_last_index();


        self.consumer_data[last_index] = ptr;
        if last_index >= self.consumer_data.capacity() {
            self.consumer_index = 0;
        } else {
            self.consumer_index += 1;
        }
        ptr
    }
    fn shutdown(&mut self) {}

    fn drop_consumer(&mut self) {}

    fn increment_consumer_count(&mut self) {}
}

impl<T, const SIZE: usize> RecyclerBuffer for StaticBuffer<T, SIZE> {
    type ItemType = T;

    #[inline]
    fn recycle(&mut self, item: NotNullItem<Self::ItemType>) {
        self.index -= 1;
        //*unsafe {self.data.get_unchecked_mut(self.index) } = item
        //println!("recycling into index {}", self.index);
        self.data[self.index] = item.as_ptr();
    }

    #[inline]
    /// takes an item from the internal buffer
    fn pop(&mut self) -> ContainerType<Self::ItemType> {
        //let item= unsafe {self.data.get_unchecked(self.index)};
        let item = self.data[self.index];
        self.index += 1;
        //*item
        item
    }

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
    fn consume_at(&self, index: usize) -> ContainerType<Self::ItemType> {
        self.consumer_data[index as usize]
    }

    #[inline]
    fn consumer_last_index(&self) -> usize {
        self.consumer_index as usize
    }

    #[inline]
    fn broadcast(&mut self) -> ContainerType<T> {
        //println!("writing to index {}, consumer index {}", self.index, self.consumer_index);
        let ptr = self.data[self.index];
        self.index += 1;

        let last_index = self.consumer_last_index();
        self.consumer_data[last_index] = ptr;
        

        self.consumer_index += 1;

        if self.consumer_index >= self.consumer_data.len() {
            self.consumer_index = 0;
        }

        unsafe { ptr.as_mut().unwrap() }
            .ref_counter
            .store(self.consumer_count, Ordering::Relaxed);
        ptr
    }

    #[inline]
    fn shutdown(&mut self) {
        let last_index = self.consumer_last_index();
        self.consumer_data[last_index] = Self::NullItem as ContainerType<Self::ItemType>;

        self.consumer_index += 1;

        if self.consumer_index >= self.consumer_data.len() {
            self.consumer_index = 0;
        }
    }

    fn drop_consumer(&mut self) {
        self.consumer_count -= 1;
    }

    fn increment_consumer_count(&mut self) {
        self.consumer_count += 1;
    }
}

// enable share behavior if the buffer is +Send and the items in the buffer are +Send
impl<B> Recycler<B>
where
    B: RecyclerBuffer + Send, <B as RecyclerBuffer>::ItemType: Send + Sync
{
    fn new(buf: GuardedBufferPtr<B>) -> Recycler<B> {
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

#[cfg(test)]
mod tests {

    use std::sync::RwLock;

    use super::*;

    #[test]
    fn single_threaded() {
        type ItemType = RwLock<u32>;

        let mut recycler = RecyclerBuilder::<ItemType>::new()
            .push(ItemType::new(100))
            .push(ItemType::new(200))
            .build();

        assert!(recycler.available() == recycler.capacity());

        let take_one = recycler.take().unwrap();
        let take_two = recycler.take().unwrap();

        assert!(*take_one.read().unwrap() == 100);
        assert!(*take_two.read().unwrap() == 200);

        assert!(recycler.take().is_none());
        assert!(recycler.available() == 0);
        assert!(recycler.capacity() == 2);

        *take_two.write().unwrap() = 600;

        drop(take_two);

        assert!(recycler.available() == 1);
        assert!(recycler.capacity() == 2);

        let take_two = recycler.take().unwrap();
        assert!(*take_two.read().unwrap() == 600);

        drop(take_two);
        drop(take_one);

        assert!(recycler.available() == 2);
        assert!(recycler.capacity() == 2);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn multi_threaded() {
        type ItemType = RwLock<Vec<u32>>;

        const ONE_SECOND: Duration = Duration::from_secs(1);

        let mut recycler = RecyclerBuilder::<ItemType>::new()
            .push(ItemType::new(vec![1, 2, 3]))
            .build();

        let (tx, rx) =
            tokio::sync::broadcast::channel::<SharedRecycleRef<DynamicBuffer<ItemType>>>(5);

        let mut handles = vec![];

        // this rx is never used but will keep any items sent on the channel from being dropped so we need to drop it
        drop(rx);

        for id in 0..5 {
            let mut thread_recv = tx.subscribe();
            handles.push(tokio::task::spawn(async move {
                while let Ok(item) = thread_recv.recv().await {
                    //println!("thread {} got {}", id, item.read().unwrap().len());
                }
            }));
        }

        let item = recycler.take().unwrap();
        assert!(recycler.available() == 0);
        assert!(recycler.take().is_none());

        item.write().unwrap().push(4);

        assert!(tx.send(item.to_shared()).is_ok());

        let new_item = recycler.wait_and_take();
        assert!(recycler.take().is_none());

        new_item.write().unwrap().clear();

        assert!(tx.send(new_item.to_shared()).is_ok());

        let new_item = recycler.wait_for(ONE_SECOND).unwrap();
        assert!(recycler.take().is_none());

        assert!(recycler.wait_for(ONE_SECOND).is_none());

        new_item.write().unwrap().push(10);

        assert!(tx.send(new_item.to_shared()).is_ok());

        // never share this item but it should still be returned to the buffer even if not shared
        let dont_share = recycler.wait_and_take();

        dont_share.write().unwrap().clear();

        // by droping then the data should be recycled
        drop(dont_share);

        assert!(recycler.available() == 1);

        println!("tests complete")
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn multi_threaded_strings() {
        use std::time::Instant;
        use tokio::sync::broadcast::channel;

        const CAPACITY: usize = 20;

        #[derive(Debug)]
        struct Item {
            name: String,
            count: u32,
        }

        unsafe impl Send for Item {}

        let fr = StaticBuffer::<Item, CAPACITY>::new([
            make_container(Item {
                name: "Item".to_string(),
                count: 0,
            }),
            make_container(Item {
                name: "Item".to_string(),
                count: 0,
            }),
            make_container(Item {
                name: "Item".to_string(),
                count: 0,
            }),
            make_container(Item {
                name: "Item".to_string(),
                count: 0,
            }),
            make_container(Item {
                name: "Item".to_string(),
                count: 0,
            }),
            make_container(Item {
                name: "Item".to_string(),
                count: 0,
            }),
            make_container(Item {
                name: "Item".to_string(),
                count: 0,
            }),
            make_container(Item {
                name: "Item".to_string(),
                count: 0,
            }),
            make_container(Item {
                name: "Item".to_string(),
                count: 0,
            }),
            make_container(Item {
                name: "Item".to_string(),
                count: 0,
            }),
            make_container(Item {
                name: "Item".to_string(),
                count: 0,
            }),
            make_container(Item {
                name: "Item".to_string(),
                count: 0,
            }),
            make_container(Item {
                name: "Item".to_string(),
                count: 0,
            }),
            make_container(Item {
                name: "Item".to_string(),
                count: 0,
            }),
            make_container(Item {
                name: "Item".to_string(),
                count: 0,
            }),
            make_container(Item {
                name: "Item".to_string(),
                count: 0,
            }),
            make_container(Item {
                name: "Item".to_string(),
                count: 0,
            }),
            make_container(Item {
                name: "Item".to_string(),
                count: 0,
            }),
            make_container(Item {
                name: "Item".to_string(),
                count: 0,
            }),
            make_container(Item {
                name: "Item".to_string(),
                count: 0,
            }),
        ]);

        let mut recycler = Recycler::<StaticBuffer<Item, CAPACITY>>::new(StaticBufferPtr::new(
            BufferControl::new(fr),
        ));

        // let mut recycler = RecyclerBuilder::<Item>::new()
        // .generate(CAPACITY, |_i| Item {name: "Item".into(), count: 0})
        // .build();

        assert!(recycler.capacity() == CAPACITY);

        let item = recycler.take().unwrap();

        assert!(item.name == "Item");
        assert!(item.count == 0);

        // we took one item so check available is one less than capacity
        assert!(recycler.available() == recycler.capacity() - 1);

        let (tx, rx) = channel::<SharedRecycleRef<StaticBuffer<Item, CAPACITY>>>(CAPACITY);
        // let (tx, rx) = channel::<SharedRecycleRef<DynamicBuffer<Item>>>(CAPACITY);

        let mut handles = vec![];

        // this rx is never used but will keep any items sent on the channel from being dropped so we need to drop it
        drop(rx);

        let thread_count = 20;
        for id in 0..thread_count {
            let mut thread_recv = tx.subscribe();
            handles.push(tokio::task::spawn(async move {
                let mut count = 0u32;
                let my_name = id.to_string();

                while let Ok(item) = thread_recv.recv().await {
                    if item.name == my_name {
                        count += item.count;
                    }
                }

                (id, count)
            }));
        }

        let mut total_count_sent = vec![0u32; thread_count];

        let iterations = 100000;

        drop(item);
        let start = Instant::now();
        for round in 1..iterations {
            tx.send(recycler.wait_and_share(|item| {
                let id = round % thread_count;
                item.name = id.to_string();
                item.count = round as u32;
                total_count_sent[id] += round as u32;
            }))
            .unwrap();
        }

        // should cause threads leave their loop when the tx end is dropped
        drop(tx);

        let results = futures::future::try_join_all(handles).await;
        let elapse = start.elapsed();

        assert!(results.is_ok());

        for result in results.ok().unwrap() {
            //println!("thread {} returning {}", result.0, result.1);
            assert!(result.1 == total_count_sent[result.0]);
        }

        println!("test completed in {:.2}s", elapse.as_secs_f32());
    }

    #[test]
    fn multi_threaded_strings2() {
        use std::time::Instant;

        const CAPACITY: usize = 20;

        #[derive(Debug)]
        struct Item {
            name: String,
            count: usize,
        }

        unsafe impl Send for Item {}

        let fr = StaticBuffer::<Item, CAPACITY>::new([
            make_container(Item {
                name: "Item".to_string(),
                count: 0,
            }),
            make_container(Item {
                name: "Item".to_string(),
                count: 0,
            }),
            make_container(Item {
                name: "Item".to_string(),
                count: 0,
            }),
            make_container(Item {
                name: "Item".to_string(),
                count: 0,
            }),
            make_container(Item {
                name: "Item".to_string(),
                count: 0,
            }),
            make_container(Item {
                name: "Item".to_string(),
                count: 0,
            }),
            make_container(Item {
                name: "Item".to_string(),
                count: 0,
            }),
            make_container(Item {
                name: "Item".to_string(),
                count: 0,
            }),
            make_container(Item {
                name: "Item".to_string(),
                count: 0,
            }),
            make_container(Item {
                name: "Item".to_string(),
                count: 0,
            }),
            make_container(Item {
                name: "Item".to_string(),
                count: 0,
            }),
            make_container(Item {
                name: "Item".to_string(),
                count: 0,
            }),
            make_container(Item {
                name: "Item".to_string(),
                count: 0,
            }),
            make_container(Item {
                name: "Item".to_string(),
                count: 0,
            }),
            make_container(Item {
                name: "Item".to_string(),
                count: 0,
            }),
            make_container(Item {
                name: "Item".to_string(),
                count: 0,
            }),
            make_container(Item {
                name: "Item".to_string(),
                count: 0,
            }),
            make_container(Item {
                name: "Item".to_string(),
                count: 0,
            }),
            make_container(Item {
                name: "Item".to_string(),
                count: 0,
            }),
            make_container(Item {
                name: "Item".to_string(),
                count: 0,
            }),
        ]);

        let mut recycler = Recycler::<StaticBuffer<Item, CAPACITY>>::new(StaticBufferPtr::new(
            BufferControl::new(fr),
        ));

        assert!(recycler.capacity() == CAPACITY);

        let item = recycler.take().unwrap();

        assert!(item.name == "Item");
        assert!(item.count == 0);

        // we took one item so check available is one less than capacity
        assert!(recycler.available() == recycler.capacity() - 1);

        let mut handles = vec![];

        let thread_count = 20;
        for id in 0..thread_count {
            let mut consumer = recycler.create_consumer();

            handles.push(std::thread::spawn(move || {
                let mut count = 0usize;
                let my_name = id.to_string();
                let mut total_events = 0;

                while let Some(item) = consumer.next() {
                    total_events += 1;
                    if item.name == my_name {
                        // if item.count != total_events {
                        //     println!("err: event #{total_events}, count was {}", item.count);
                        // }
                        //println!("thread {} adding {} to {}", my_name, item.count, count);
                        count += item.count;
                    }
                }

                (id, count, total_events)
            }));
        }

        let mut total_count_sent = vec![0usize; thread_count];

        let iterations = 100_000;

        drop(item);
        let start = Instant::now();
        for round in 1..iterations {
            recycler.wait_and_broadcast(|v| {
                let id = round % thread_count;
                v.name = id.to_string();
                v.count = round;
                total_count_sent[id] += round as usize;
            });
        }

        recycler.shutdown();
        let mut results = vec![];

        for result in handles.into_iter() {
            results.push(result.join());
        }

        let elapse = start.elapsed();
        println!("test completed in {:.2}s", elapse.as_secs_f32());
        println!("Waiting for threads");
        for result in results.into_iter() {
            match result {
                Ok((id, value, total_events)) => {
                    let expected = total_count_sent[id];
                    //println!("thread {} expecting {} and had {} total_events", id, expected, total_events);
                    assert!(
                        value == expected,
                        "thread {id}, expected {expected} but got {value}"
                    );
                    assert!(total_events == (iterations -1));
                }
                Err(e) => {
                    eprintln!("Error: {e:?}");
                }
            }
        }
    }

    #[test]
    fn drop_test() {
        // this test expected drop behavior. Since dropping buffer items should be recycled until
        // the recycler AND all outstanding item references are dropped.

        type CounterType = Arc<Mutex<u32>>;

        let instance_counter = CounterType::new(Mutex::new(0u32));

        struct DropItem {
            data: u32,
            counter: CounterType,
        }

        impl DropItem {
            fn new(counter: CounterType) -> Self {
                let item = DropItem { data: 0, counter };
                *item.counter.lock().unwrap() += 1;
                item
            }
        }
        impl Drop for DropItem {
            fn drop(&mut self) {
                *self.counter.lock().unwrap() -= 1;
            }
        }

        let mut recycler = RecyclerBuilder::new()
            .push(DropItem::new(instance_counter.clone()))
            .push(DropItem::new(instance_counter.clone()))
            .build();

        // we should have as many instances as our buffer capacity which should be 2
        assert!(*instance_counter.lock().unwrap() == recycler.capacity() as u32);

        let mut take_one = recycler.take().unwrap();
        let mut take_two = recycler.take().unwrap();

        // dropping the recycler should not cause issues with references already taken
        drop(recycler);

        assert!(*instance_counter.lock().unwrap() == 2);

        // set data on our perfectly valid references
        take_one.data = 100u32;
        take_two.data = 200u32;

        drop(take_one);
        // even though the item is dropped the inner recycle buffer is still alive which keeps
        // all items alive until the last one is dropped
        assert!(*instance_counter.lock().unwrap() == 2);

        // drop the last item from the buffer and now all of them should be dropped
        drop(take_two);
        assert!(*instance_counter.lock().unwrap() == 0);

        // now lets test the other scenario where the recycler is the last to drop
        let mut recycler = RecyclerBuilder::new()
            .push(DropItem::new(instance_counter.clone()))
            .push(DropItem::new(instance_counter.clone()))
            .push(DropItem::new(instance_counter.clone()))
            .build();

        assert!(*instance_counter.lock().unwrap() == recycler.capacity() as u32);

        let take_one = recycler.take().unwrap();
        let take_two = recycler.take().unwrap();

        assert!(*instance_counter.lock().unwrap() == recycler.capacity() as u32);

        drop(take_one);

        assert!(*instance_counter.lock().unwrap() == recycler.capacity() as u32);

        drop(take_two);
        assert!(*instance_counter.lock().unwrap() == recycler.capacity() as u32);

        // dropping the recycler should finally drop the items, even ones that were never taken
        drop(recycler);
        assert!(*instance_counter.lock().unwrap() == 0);

        // now lets test shared item drops
        let mut recycler = RecyclerBuilder::new()
            .push(DropItem::new(instance_counter.clone()))
            .push(DropItem::new(instance_counter.clone()))
            .push(DropItem::new(instance_counter.clone()))
            .build();

        let shared_one = recycler.take().unwrap().to_shared();
        let shared_two = recycler.take().unwrap().to_shared();
        assert!(*instance_counter.lock().unwrap() == recycler.capacity() as u32);
        drop(shared_two);
        assert!(*instance_counter.lock().unwrap() == recycler.capacity() as u32);

        drop(shared_one);
        assert!(*instance_counter.lock().unwrap() == recycler.capacity() as u32);

        let shared_three = recycler.take().unwrap().to_shared();
        let shared_four = recycler.take().unwrap().to_shared();

        drop(recycler);
        assert!(*instance_counter.lock().unwrap() == 3);

        drop(shared_three);
        assert!(*instance_counter.lock().unwrap() == 3);

        // last to drop will drop the internal buffer
        drop(shared_four);
        assert!(*instance_counter.lock().unwrap() == 0);
    }
}
