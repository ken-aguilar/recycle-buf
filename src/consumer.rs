use std::{ptr::NonNull, ops::Deref};

use crate::buffer::{RecyclerBuffer, GuardedBufferPtr, NotNullItem};


pub struct Consumer<B>
where
    B: RecyclerBuffer,
{
    buf_ptr: GuardedBufferPtr<B>,
    consumer_index: usize,
}

impl<B> Consumer<B>
where
    B: RecyclerBuffer + Send,
    <B as RecyclerBuffer>::ItemType: Send + Sync,
{

    pub fn new(buf_ptr: GuardedBufferPtr<B>, consumer_index: usize) -> Consumer<B> {
        Consumer {
            buf_ptr,
            consumer_index,
        }
    } 

    #[inline]
    pub fn next(&mut self) -> Option<ConsumerRef<B>> {

        let locked_buffer = self
            .buf_ptr
            .item_produced_event
            .wait_while(self.buf_ptr.buffer.lock().unwrap(), |e| {
                self.consumer_index == e.consumer_last_index()
            })
            .unwrap();

        let item_counter = locked_buffer.consume_at(self.consumer_index);

        if item_counter.is_null() {
            return None;
        }
        //  println!("reading index {}, last is {}", self.consumer_index, locked_buffer.consumer_last_index());


        if self.consumer_index == locked_buffer.capacity() {
            self.consumer_index = 0;
        } else {
            self.consumer_index += 1;
        }

        Some(ConsumerRef {
            consumer: self,
            item_ptr: unsafe {NonNull::new_unchecked(item_counter)},
        })
    }
}

pub struct ConsumerRef<'a, B>
where
    B: RecyclerBuffer,
{
    consumer: &'a Consumer<B>,
    item_ptr: NotNullItem<<B as RecyclerBuffer>::ItemType>,
}

impl<'a, B> Deref for ConsumerRef<'a, B>
where
    B: RecyclerBuffer,
{
    type Target = <B as RecyclerBuffer>::ItemType;

    #[inline]
    fn deref(&self) -> &Self::Target {
        unsafe { &self.item_ptr.as_ref().item }
    }
}

impl<'a, B> Drop for ConsumerRef<'a, B>
where
    B: RecyclerBuffer,
{
    fn drop(&mut self) {
        let mut ptr = self.item_ptr;
        if unsafe { ptr.as_mut().decrement() } == 1 {
            // we last
            self.consumer.buf_ptr.buffer.lock().unwrap().recycle(ptr);
            self.consumer.buf_ptr.recycled_event.notify_one();
        }
    }
}

