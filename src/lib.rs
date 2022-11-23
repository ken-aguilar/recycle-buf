use std::{sync::{Arc, Condvar, Mutex, atomic::{Ordering, AtomicU32}}, ptr::NonNull, ops::{Deref, DerefMut}, time::Duration, borrow::{self}};

type RecyclerInnerType<T> = Arc<(Mutex<RecyclerInner<T>>, Condvar)>;

type ContentType<T> = NonNull<ItemHolder<T>>;


pub struct RecyclerBuilder<T> {
    contents: Vec<ContentType<T>>
}

struct ItemHolder<T> {
    ref_counter: AtomicU32,
    item: T,
}

impl<T> Default for RecyclerBuilder<T> {
    fn default() -> Self {
         Self::new()
    }    
}

impl<T> RecyclerBuilder<T> {
    pub fn new() -> Self {
        RecyclerBuilder { contents: vec![] }
    }

    pub fn push(mut self, item: T) -> Self {
        // move the item to the heap via box
        let boxed = Box::new(ItemHolder {item, ref_counter: AtomicU32::new(0)});

        // get a pointer from the heap
        self.contents.push(NonNull::new(Box::into_raw(boxed)).expect("null ptr"));
        Self {contents: self.contents}
    }

    pub fn build(self) -> Recycler<T> {

        let contents = self.contents.into_boxed_slice();

        Recycler{
            inner: RecyclerInnerType::new((
                Mutex::new(
                    RecyclerInner{
                        data: contents,
                        index: 0
                    }
                ),
                Condvar::new()
            ))
        }
    }
}

pub struct Recycler<T> {
    inner: RecyclerInnerType<T>
}

#[derive(Debug)]
struct RecyclerInner<T> {    
    data: Box<[ContentType<T>]>,
    index: usize,
}

impl<T> Drop for RecyclerInner<T> {
    fn drop(&mut self) {
        for i in 0 ..self.data.len() {
            let boxed = unsafe {Box::from_raw(self.data[i].as_ptr())};
            drop(boxed)
        }
    }
}

impl<T> RecyclerInner<T> {

    #[inline]
    fn recycle(&mut self, item: ContentType<T>) {
        self.index -= 1;
        self.data[self.index] = item;
    }

    #[inline]
    fn pop(&mut self) -> ContentType<T>{
        let item= self.data[self.index];        
        self.index += 1;
        item
    }

    #[inline]
    fn available(&self) -> usize {
        self.data.len() - self.index
    }
}

impl <T> Recycler<T> {

    #[inline]
    fn make_ref(&self, item_ptr: ContentType<T>) -> RecycleRef<T> {
        RecycleRef {
            buf_ptr: self.inner.clone(),
            item_ptr: item_ptr.as_ptr(),
        }
    }

    pub fn take(&mut self)-> Option<RecycleRef<T>> {
        let mut inner = self.inner.0.lock().unwrap();
        if inner.index == inner.data.len(){
            None
        } else {
            Some(self.make_ref(inner.pop()))
        }
    }

    #[inline]
    pub fn wait_and_share<F: FnOnce(&mut T)>(&mut self, f: F) -> SharedRecycleRef<T> {
        let mut inner = self.inner.0.lock().unwrap();
        // while full wait until one is available
        while inner.index == inner.data.len() {
            inner = self.inner.1.wait(inner).unwrap();
        }
            
        let mut ptr = inner.pop();
        // drop lock before calling user function
        drop(inner);

        f(unsafe{&mut ptr.as_mut().item});

        SharedRecycleRef::new(self.inner.clone(), ptr)

    } 

    /// waits for one item to be available in the buffer. 
    pub fn wait_and_take(&self)-> RecycleRef<T> {
        let mut inner = self.inner.0.lock().unwrap();
        while inner.index == inner.data.len() {
            inner = self.inner.1.wait(inner).unwrap();
        }
            
        let ptr = inner.pop();
        drop(inner);

        self.make_ref(ptr)
    }

    pub fn wait_for(&self, dur:  Duration) -> Option<RecycleRef<T>>  {
        let inner = self.inner.0.lock().unwrap();
        // loop until one is available
        if let Ok((mut inner, timeout)) = self.inner.1.wait_timeout_while(inner, dur, |e| {e.index == e.data.len()}) {
            if !timeout.timed_out() {
                // we didn't time out so at least one item is available
                let ptr = inner.pop();

                // we don't need the lock any more so release it
                drop(inner);
                        
                return Some(self.make_ref(ptr))
            }
        }
        None
    }

    #[inline]
    pub fn available(&self) -> usize {
        self.inner.0.lock().unwrap().available()
    }

    #[inline]
    pub fn capacity(&self) -> usize {
        self.inner.0.lock().unwrap().data.len()
    }


}

pub struct RecycleRef<T> {
    buf_ptr: RecyclerInnerType<T>,
    item_ptr: * mut ItemHolder<T>,
}


impl<T> RecycleRef<T> {

    /// consume this ref into a shareable form
    pub fn to_shared(mut self) -> SharedRecycleRef<T> {

        let ptr = std::mem::replace(&mut self.item_ptr, std::ptr::null::<ItemHolder<T>>() as * mut ItemHolder<T>);
        SharedRecycleRef::new(self.buf_ptr.clone(), NonNull::new(ptr).unwrap())

    }

}

impl<T> Deref for RecycleRef<T> {
    type Target = T;

    #[inline]
    fn deref(&self) -> &Self::Target {
        // item ptr is only null after consumed by to_shared
        unsafe {
            &self.item_ptr.as_ref().unwrap().item
        }
    }
}

impl<T> DerefMut for RecycleRef<T> {

    #[inline]
    fn deref_mut(&mut self) -> &mut Self::Target {
        unsafe {
            &mut self.item_ptr.as_mut().unwrap().item
        }
    }
}

impl<T> Drop for RecycleRef<T> {
    fn drop(&mut self) {

        if !self.item_ptr.is_null() {
            println!("single man standing");
            // we are the last reference remaining. We are now responsible for returning the
            // data to the main buffer
            let mut inner = self.buf_ptr.0.lock().unwrap();
            inner.recycle(NonNull::new(self.item_ptr).unwrap());
            self.buf_ptr.1.notify_one();
        }

    }
}

#[derive(Debug)]
pub struct SharedRecycleRef<T> {
    buf_ptr: RecyclerInnerType<T>,
    item_ptr: ContentType<T>,
}

impl<T> SharedRecycleRef<T> {

    fn new(buf_ptr: RecyclerInnerType<T>, item_ptr: ContentType<T>) -> Self {
        unsafe {
            item_ptr.as_ref().ref_counter.store(1, Ordering::Relaxed);
        }
        SharedRecycleRef { 
            buf_ptr, 
            item_ptr, 
        }
    }
}

impl<T> Deref for SharedRecycleRef<T> {
    type Target = T;

    #[inline]
    fn deref(&self) -> &Self::Target {
        unsafe {
            &self.item_ptr.as_ref().item
        }
    }
}

impl<T> borrow::Borrow<T> for SharedRecycleRef<T> {
    fn borrow(&self) -> &T {
        self
    }
}

impl<T> Clone for SharedRecycleRef<T> {

    #[inline]
    fn clone(&self) -> Self {
        let counter = unsafe { &self.item_ptr.as_ref().ref_counter };

        let old_rc = counter.fetch_add(1, Ordering::Relaxed);

        if old_rc >= isize::MAX as u32 {
            std::process::abort();
        }

        SharedRecycleRef { buf_ptr: self.buf_ptr.clone(), item_ptr: self.item_ptr}
    }
}

impl<T> Drop for SharedRecycleRef<T> {

    #[inline]
    fn drop(&mut self) {

        let counter = unsafe { &self.item_ptr.as_ref().ref_counter };

        let value = counter.fetch_sub(1, Ordering::Release);
        //println!("drop checking: {}", value);

        if value != 1 {
            return;
        }

        // we are the last reference remaining. We are now responsible for returning the
        // data to the recycler
        let mut inner = self.buf_ptr.0.lock().unwrap();
        inner.recycle(self.item_ptr);
        self.buf_ptr.1.notify_one();

    }
}

unsafe impl<T: Sync + Send> Send for SharedRecycleRef<T> {}
unsafe impl<T: Sync + Send> Sync for SharedRecycleRef<T> {}

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

        let (tx, rx) = tokio::sync::broadcast::channel::<SharedRecycleRef<ItemType>>(5);

        let mut handles = vec![];

        // this rx is never used but will keep any items sent on the channel from being dropped so we need to drop it
        drop(rx);

        for id in 0..5 {

            let mut thread_recv = tx.subscribe();
            handles.push(tokio::task::spawn( async move {


                while let Ok(item) =  thread_recv.recv().await {
                    println!("thread {} got {}", id, item.read().unwrap().len());
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

        #[derive(Debug)]
        struct Item {
            name: String,
            count: u32
        }

        let mut recycler = RecyclerBuilder::<Item>::new()
        .push(Item {name: "Item".into(), count: 0})
        .push(Item {name: "Item".into(), count: 0})
        .push(Item {name: "Item".into(), count: 0})
        .push(Item {name: "Item".into(), count: 0})
        .push(Item {name: "Item".into(), count: 0})
        .push(Item {name: "Item".into(), count: 0})
        .push(Item {name: "Item".into(), count: 0})
        .push(Item {name: "Item".into(), count: 0})
        .push(Item {name: "Item".into(), count: 0})
        .push(Item {name: "Item".into(), count: 0})
        .build();

        let item = recycler.take().unwrap();

        assert!(item.name == "Item");
        assert!(item.count == 0);

        // we took one item so check available is one less than capacity
        assert!(recycler.available() == recycler.capacity() -1 );

        let (tx, rx) = tokio::sync::broadcast::channel::<SharedRecycleRef<Item>>(10);

        let mut handles = vec![];

        // this rx is never used but will keep any items sent on the channel from being dropped so we need to drop it
        drop(rx);

        let thread_count = 20;
        for id in 0..thread_count {

            let mut thread_recv = tx.subscribe();
            handles.push(tokio::task::spawn( async move {

                let mut count = 0u32;
                let my_name = id.to_string();

                while let Ok(item) =  thread_recv.recv().await {
                    if item.name == my_name {
                        count += item.count;
                    }
                }

                (id, count)
            }));
        }

        let mut total_count_sent = vec![0u32; thread_count];

        drop(item);
        let start = Instant::now();
        for round in 1 .. 50000 {
            tx.send(recycler.wait_and_share(|item| {                
                let id = round % thread_count;
                item.name = id.to_string();
                item.count = round as u32;
                total_count_sent[id] += round as u32;                
            })).unwrap();           
        }

        // should cause threads leave their loop when the tx end is dropped
        drop(tx);

        let results = futures::future::try_join_all(handles).await;
        let elapse = start.elapsed();

        assert!(results.is_ok());

        for result in results.ok().unwrap() {
            println!("thread {} returning {}", result.0, result.1);
            assert!(result.1 == total_count_sent[result.0]);
        }

        println!("test completed in {:.2}s", elapse.as_secs_f32());

    }

    #[test]
    fn drop_test() {
        // this test expected drop behavior. Since dropping buffer items should be recycled until
        // the recycler AND all outstanding item references are dropped. 

        type CounterType = Arc<Mutex<u32>>;

        let instance_counter = CounterType::new(Mutex::new(0u32));

        struct DropItem {
            data: u32,
            counter: CounterType
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
