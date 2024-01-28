use std::{ops::Deref, ptr::NonNull};

use crate::buffer::{
    ContainerType, GuardedBufferPtr, NotNullItem, ProducerConsumerBuffer, RecyclerBuffer,
};

pub struct Consumer<B>
where
    B: ProducerConsumerBuffer + Send,
    <B as RecyclerBuffer>::ItemType: Send + Sync,
{
    buf_ptr: GuardedBufferPtr<B>,
    consumer_counter: usize,
    shutdown: bool,
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
    pub fn new(buf_ptr: GuardedBufferPtr<B>) -> Self {
        Self {
            buf_ptr,
            consumer_counter: 1,
            shutdown: false
        }
    }

    #[inline]
    pub fn next_item(&mut self) -> Option<ConsumerRef<B>> {
        if self.shutdown {
            return None;
        }

        let Some(item_counter_ptr) = self.buf_ptr.get().consume_next(self.consumer_counter) else {
            self.shutdown = true;
            return None;
        };

        self.consumer_counter += 1;


        Some(ConsumerRef {
            consumer: self,
            item_ptr: unsafe {
                NonNull::new_unchecked(
                    item_counter_ptr as *mut ContainerType<<B as RecyclerBuffer>::ItemType>,
                )
            },
        })
    }

    #[inline]
    pub fn next_item_fn<F>(&mut self, f: F) -> bool
    where
        F: FnOnce(&<B as RecyclerBuffer>::ItemType),
    {

        if self.shutdown {
            return false;
        }

        let Some(item_counter) = self.buf_ptr.get().consume_next(self.consumer_counter) else {
            self.shutdown = true;
            return false;
        };

        self.consumer_counter += 1;

        f(&item_counter.item);


        item_counter.decrement_ref_count();
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
        unsafe {
            ptr.as_mut().decrement_ref_count();
        }
    }
}
