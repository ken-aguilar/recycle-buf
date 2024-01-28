use std::{sync::Arc, time::Duration};

use recycle_buf::{
    buffer::{DynamicBuffer, ItemCounter, SharedRecycleRef, SequenceBuffer, StaticBufferPtr},
    recycler::{Recycler, RecyclerBuilder},
    sync_cell::SyncUnsafeCell,
};

const CAPACITY: usize = 8;
const ITERATIONS: usize = 2_500_000;

#[derive(Debug, Clone)]
struct TestItem {
    name: String,
    count: usize,
    sent: usize,
}
fn setup() -> Recycler<SequenceBuffer<TestItem, [ItemCounter<TestItem>; CAPACITY]>> {


    let fixed: [ItemCounter<TestItem>; CAPACITY] = std::array::from_fn(|_i| ItemCounter::new(TestItem{
        count: 0,
        name: "Item".to_string(),
        sent: 0
    }));

    Recycler::new(StaticBufferPtr::new(SyncUnsafeCell::new(SequenceBuffer::new(fixed))))
}

async fn multi_threaded_strings_tokio() {
    use std::time::Instant;
    use tokio::sync::broadcast::channel;

    let mut recycler = RecyclerBuilder::<TestItem>::new()
        .generate(CAPACITY, |_i| TestItem {
            name: "Item".to_string(),
            count: 0,
            sent: 0,
        })
        .build();
    assert!(recycler.capacity() == CAPACITY);

    let item = recycler.take().unwrap();

    assert!(item.name == "Item");
    assert!(item.count == 0);

    // we took one item so check available is one less than capacity
    assert!(recycler.available() == recycler.capacity() - 1);

    let (tx, rx) = channel::<SharedRecycleRef<DynamicBuffer<TestItem>>>(CAPACITY);
    // let (tx, rx) = channel::<SharedRecycleRef<DynamicBuffer<Item>>>(CAPACITY);

    let mut handles = vec![];

    // this rx is never used but will keep any items sent on the channel from being dropped so we need to drop it
    drop(rx);

    let thread_count = 7;
    for id in 0..thread_count {
        let mut thread_recv = tx.subscribe();
        handles.push(tokio::task::spawn(async move {
            let mut count = 0usize;
            let my_name = id.to_string();
            let mut total_events = 0;

            while let Ok(item) = thread_recv.recv().await {
                total_events += 1;
                if item.name == my_name {
                    count += item.count;
                }
            }

            (id, count, total_events)
        }));
    }

    let mut total_count_sent = vec![0usize; thread_count];

    drop(item);
    let start = Instant::now();
    for round in 1..ITERATIONS {
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

    let results = futures::future::try_join_all(handles).await;
    let elapse = start.elapsed();

    assert!(results.is_ok());

    for result in results.ok().unwrap() {
        //println!("thread {} returning {}", result.0, result.1);
        assert!(result.1 == total_count_sent[result.0]);
    }

    println!("test completed in {:.3}s", elapse.as_secs_f32());
}

fn multi_threaded_strings_custom() {
    use std::time::Instant;

    let mut recycler = setup();

    assert!(recycler.capacity() == CAPACITY);

    let mut handles = vec![];

    let thread_count = 7;
   // let barrier = Arc::new(std::sync::Barrier::new(thread_count + 1));

    for id in 0..thread_count {
        let mut consumer = recycler.create_consumer();
        // let b = barrier.clone();
        handles.push(std::thread::spawn(move || {
     //       b.wait();
            let mut count = 0usize;
            let my_name = id.to_string();
            let mut total_events = 0;
            loop {
                if !consumer.next_item_fn(|item| {
                    total_events += 1;
                    if item.name == my_name {
                        // if item.count != total_events {
                        //     println!("{:?}: err: event #{total_events}, count was {}", std::thread::current().id(), item.count);
                        //     panic!("bailing");
                        // }
                        // println!("thread {} adding {} to {}", my_name, item.count, count);
                        count += item.count;
                    }
                }) {
                    // println!("thread stopping!!");
                    break;
                }
            }

            (id, count, total_events)
        }));
    }

   // barrier.wait();

    let mut total_count_sent = vec![0usize; thread_count];

    let start = Instant::now();
    for round in 1..ITERATIONS {
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

    println!(
        "test producer consumer completed in {:.3}s",
        elapse.as_secs_f32()
    );
    println!("Waiting for threads");
    for result in results.into_iter() {
        match result {
            Ok((id, value, total_events)) => {
                let expected = total_count_sent[id];
                //    println!("thread {} expecting {}, got {} and had {} total_events", id, expected, value, total_events);
                assert!(
                    value == expected,
                    "thread {id}, expected {expected} but got {value}"
                );
                assert!(total_events == (ITERATIONS - 1));
            }
            Err(e) => {
                eprintln!("Error: {e:?}");
            }
        }
    }
}

async fn multi_threaded_strings_tokio_no_rec() {
    use std::time::Instant;
    use tokio::sync::broadcast::channel;

    let (tx, rx) = channel::<TestItem>(ITERATIONS);

    let mut handles = vec![];

    let thread_count = 7;
    let barrier = Arc::new(tokio::sync::Barrier::new(thread_count + 1));
    for id in 0..thread_count {
        let bar = barrier.clone();

        let mut thread_recv = tx.subscribe();
        handles.push(tokio::task::spawn(async move {
            bar.wait().await;
            let mut count = 0usize;
            let my_name = id.to_string();

            while let Ok(item) = thread_recv.recv().await {
                if item.name == my_name {
                    count += item.count;
                }
            }

            (id, count)
        }));
    }
    // this rx is never used but will keep any items sent on the channel from being dropped so we need to drop it
    drop(rx);

    let mut total_count_sent = vec![0usize; thread_count];

    let start = Instant::now();
    barrier.wait().await;
    for round in 1..ITERATIONS {
        let id = round % thread_count;
        tx.send(TestItem {
            name: id.to_string(),
            count: round as usize,
            sent: 0,
        })
        .unwrap();
        total_count_sent[id] += round as usize;
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

fn main() {
    println!(
        "Num threads {}",
        std::thread::available_parallelism().expect("failed to get threads")
    );
    multi_threaded_strings_custom();

    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_time()
        .build()
        .expect("could npt build rt");

    rt.block_on(multi_threaded_strings_tokio());

    rt.block_on(multi_threaded_strings_tokio_no_rec())
}
