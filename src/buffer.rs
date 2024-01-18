use std::{sync::{atomic::{AtomicU32, Ordering}, Mutex, Condvar, Arc}, ptr::NonNull, ops::{Deref, DerefMut}};


pub type NotNullItem<T> = NonNull<ItemCounter<T>>;
pub type ContainerType<T> = * mut ItemCounter<T>;
pub type GuardedBufferPtr<B> = Arc<BufferControl<B>>;

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
    ref_counter: AtomicU32,
    pub item: T,
}

unsafe impl<T> Send for ItemCounter<T> where T: Send {}

impl<T> ItemCounter<T> {
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

#[derive(Debug)]
pub struct BufferControl<B>
where
    B: RecyclerBuffer,
{
    pub buffer: Mutex<B>,
    pub recycled_event: Condvar,
    pub item_produced_event: Condvar,
}

impl<B> BufferControl<B>
where
    B: RecyclerBuffer,
{
    pub fn new(buffer: B) -> BufferControl<B> {
        BufferControl {
            buffer: Mutex::new(buffer),
            recycled_event: Condvar::new(),
            item_produced_event: Condvar::new(),
        }
    }
}

#[derive(Debug)]
pub struct DynamicBuffer<T> {
    data: Box<[ContainerType<T>]>,
    index: usize,
    consumer_index: u32,
    consumer_data: Vec<ContainerType<T>>,
}

impl<T> DynamicBuffer<T> {
    pub fn new(data: Box<[ContainerType<T>]>) -> DynamicBuffer<T> {
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
    pub fn new(data: [ContainerType<T>; SIZE]) -> StaticBuffer<T, SIZE> {
        let consumer_data = vec![Self::NULL_ITEM as ContainerType<T>; data.len() + 1];

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
        self.consumer_data[last_index] = Self::NULL_ITEM as ContainerType<Self::ItemType>;

        self.consumer_index += 1;

        if self.consumer_index >= self.consumer_data.len() {
            self.consumer_index = 0;
        }
    }

    #[inline]
    fn drop_consumer(&mut self) {
        self.consumer_count -= 1;
    }

    #[inline]
    fn increment_consumer_count(&mut self) {
        self.consumer_count += 1;
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
    pub fn new(buf_ptr: GuardedBufferPtr<B>, item_ptr: NotNullItem<<B as RecyclerBuffer>::ItemType>) -> RecycleRef<B> {
        RecycleRef {
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
