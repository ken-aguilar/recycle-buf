use std::{ops::Deref, ptr::NonNull};

use crate::buffer::{GuardedBufferPtr, NotNullItem, ProducerConsumerBuffer, RecyclerBuffer};

pub struct Consumer<B>
where
    B: ProducerConsumerBuffer + Send,
    <B as RecyclerBuffer>::ItemType: Send + Sync,
{
    buf_ptr: GuardedBufferPtr<B>,
    consumer_index: usize,
}

unsafe impl<B> Send for Consumer<B>
where
    B: ProducerConsumerBuffer + Send,
    <B as RecyclerBuffer>::ItemType: Send + Sync,
{
}

impl<B> Consumer<B>
where
    B: ProducerConsumerBuffer + Send,
    <B as RecyclerBuffer>::ItemType: Send + Sync,
{

    pub fn new(buf_ptr: GuardedBufferPtr<B>, consumer_index: usize) -> Self {
        Self {
            buf_ptr,
            consumer_index,
        }
    }

    #[inline]
    pub fn next(&mut self) -> Option<ConsumerRef<B>> {
        let (item_counter_ptr, recycle_to) = self.buf_ptr.get().consume_at(&mut self.consumer_index);

        if item_counter_ptr.is_null() {
            return None;
        }

        Some(ConsumerRef {
            consumer: self,
            item_ptr: unsafe { NonNull::new_unchecked(item_counter_ptr.cast_mut()) },
            recycle_to
        })
    }

    #[inline]
    pub fn next_fn<F>(&mut self, f: F) -> bool
    where
        F: FnOnce(&<B as RecyclerBuffer>::ItemType),
    {
        let (item_counter_ptr, recycle_to) = self.buf_ptr.get().consume_at(&mut self.consumer_index);

        if item_counter_ptr.is_null() {
            return false;
        }
        let item_counter = unsafe { item_counter_ptr.as_ref().unwrap() };
        f(&item_counter.item);

        let left = item_counter.decrement();
        println!("consumed item at {recycle_to}, count left is {}", left - 1);
        if left == 1 {
            println!("attempting to reycle to {recycle_to}");
            self.buf_ptr
            .get()
            .recycle(recycle_to);
        }
        true
    }
}

pub struct ConsumerRef<'a, B>
where
    B: ProducerConsumerBuffer + Send,
    <B as RecyclerBuffer>::ItemType: Send + Sync,
{
    consumer: &'a Consumer<B>,
    item_ptr: NotNullItem<<B as RecyclerBuffer>::ItemType>,
    recycle_to: usize
}

impl<'a, B> Deref for ConsumerRef<'a, B>
where
    B: ProducerConsumerBuffer + Send,
    <B as RecyclerBuffer>::ItemType: Send + Sync,
{
    type Target = <B as RecyclerBuffer>::ItemType;

    #[inline]
    fn deref(&self) -> &Self::Target {
        unsafe { &self.item_ptr.as_ref().item }
    }
}

impl<'a, B> Drop for ConsumerRef<'a, B>
where
    B: ProducerConsumerBuffer + Send,
    <B as RecyclerBuffer>::ItemType: Send + Sync,
{
    fn drop(&mut self) {
        let mut ptr = self.item_ptr;
        if unsafe { ptr.as_mut().decrement() } == 1 {
            // we last
            self.consumer.buf_ptr.get().recycle(self.recycle_to);
        }
    }
}
