use std::{ops::Deref, ptr::NonNull};

use crate::buffer::{GuardedBufferPtr, NotNullItem, ProducerConsumerBuffer, RecyclerBuffer, ContainerType};

pub struct Consumer<B>
where
    B: ProducerConsumerBuffer + Send,
    <B as RecyclerBuffer>::ItemType: Send + Sync,
{
    buf_ptr: GuardedBufferPtr<B>,
    consumer_counter: usize
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
            consumer_counter: 0
        }
    }

    #[inline]
    pub fn next(&mut self) -> Option<ConsumerRef<B>> {

        let Some(item_counter_ptr) = self.buf_ptr.get().consume_next(self.consumer_counter) else {
            return None;
        };

        self.consumer_counter += 1;
                
        if item_counter_ptr.decrement() == 1 {
            // println!("attempting to reycle seq {:?}", item_counter.sequence);
            item_counter_ptr.recycle();
        }

        Some(ConsumerRef {
            consumer: self,
            item_ptr: unsafe { NonNull::new_unchecked(item_counter_ptr as *mut ContainerType<<B as RecyclerBuffer>::ItemType>) },
        })
    }

    #[inline]
    pub fn next_fn<F>(&mut self, f: F) -> bool
    where
        F: FnOnce(&<B as RecyclerBuffer>::ItemType),
    {

        let buf_ptr = self.buf_ptr.get();

        let Some(item_counter) = buf_ptr.consume_next(self.consumer_counter) else {
            return false;
        };

        self.consumer_counter += 1;

        f(&item_counter.item);
                
        // println!("{:?} consuming {}, rf cnt {}", std::thread::current().id(), self.consumer_counter, left);

        if item_counter.decrement() == 1 {
            // println!("attempting to reycle seq {:?}", item_counter.sequence);
            item_counter.recycle();
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
            if  ptr.as_mut().decrement() == 1 {
                // we last
                self.item_ptr.as_mut().recycle();
            }

        }
    }
}
