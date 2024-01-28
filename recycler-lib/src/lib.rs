//! This module implements a recycler buffer system that allows for pre-allocation and memory reuse
//! by leveraging the Rust language's drop mechanics. The main motivation for this system is avoiding
//! memory allocations of large and/or expensive to create objects in a realtime environment

pub mod buffer;
pub mod consumer;
pub mod recycler;
pub mod sync_cell;

#[cfg(test)]
mod tests {

    use std::sync::{Arc, Mutex, RwLock};
    use std::time::Duration;

    use crate::buffer::SharedRecycleRef;
    use crate::{buffer::DynamicBuffer, recycler::RecyclerBuilder};

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

        for _id in 0..5 {
            let mut thread_recv = tx.subscribe();
            handles.push(tokio::task::spawn(async move {
                while let Ok(_item) = thread_recv.recv().await {
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

    #[ignore = "crashing"]
    #[test]
    fn multi_threaded_strings_crossbeam() {
        use crossbeam::channel;
        use std::time::Instant;

        const CAPACITY: usize = 32;

        let mut recycler = RecyclerBuilder::<TestItem>::new()
            .generate(CAPACITY, |_i| TestItem {
                name: "Item".to_string(),
                count: 0,
            })
            .build();

        assert!(recycler.capacity() == CAPACITY);

        let item = recycler.take().unwrap();

        assert!(item.name == "Item");
        assert!(item.count == 0);

        // we took one item so check available is one less than capacity
        assert!(recycler.available() == recycler.capacity() - 1);

        let (tx, rx) = channel::bounded::<SharedRecycleRef<DynamicBuffer<TestItem>>>(CAPACITY);

        let mut handles = vec![];

        // this rx is never used but will keep any items sent on the channel from being dropped so we need to drop it

        let thread_count = 7;
        for id in 0..thread_count {
            let thread_recv = rx.clone();
            handles.push(std::thread::spawn(move || {
                let mut count = 0usize;
                let my_name = id.to_string();
                let mut total_events = 0usize;

                while let Ok(item) = thread_recv.recv() {
                    println!("{:?}({}): recv'd {:?}", std::thread::current().id(), my_name, *item);

                    total_events += 1;
                    if item.name == my_name {
                        if item.count != total_events {
                            println!("err: event #{total_events}, count was {}", item.count);
                        }
                        println!("thread {} adding {} to {}", my_name, item.count, count);
                        count += item.count as usize;
                    }
                }

                (id, count, total_events)
            }));
        }
        drop(rx);

        let mut total_count_sent = vec![0usize; thread_count];

        let iterations = 5;

        drop(item);
        let start = Instant::now();
        for round in 1..iterations {
            tx.send(recycler.wait_and_share(|item| {
                let id = round % thread_count;
                item.name = id.to_string();
                item.count = round as usize;
                total_count_sent[id] += round as usize;
            }))
            .unwrap();
        }

        // should cause threads leave their loop when the tx end is dropped
        drop(tx);

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
                    println!(
                        "thread {} expecting {} and had {} total_events",
                        id, expected, total_events
                    );
                    assert!(
                        value == expected,
                        "thread {id}, expected {expected} but got {value}"
                    );
                    assert!(total_events == (iterations - 1));
                }
                Err(e) => {
                    eprintln!("Error: {e:?}");
                }
            }
        }

        println!("test completed in {:.2}s", elapse.as_secs_f32());
    }

    #[derive(Debug, Clone)]
    struct TestItem {
        name: String,
        count: usize,
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
