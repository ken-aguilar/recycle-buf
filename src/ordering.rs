
#[derive(Debug)]
pub(crate) struct BufferState {

    available: usize,
    available_order: usize,
    pending: usize,

    all_bits: usize,
    size: u32

}

impl BufferState {
    pub(crate) fn new(size: u32) -> Self {
        let all_bits = usize::MAX.checked_shr(64-size).expect("limited to 64");

        // println!("all bits {:064b}", all_bits);

        Self {
            available: all_bits,
            available_order: all_bits,
            pending: 0,
            all_bits,
            size 
        }
    }

    pub(crate) fn return_one(&mut self, index: u32) {
        let pending_bit = 1usize << (self.size - index -1);

//        assert!((self.pending & pending_bit) > 0);

        if (self.pending & pending_bit) == 0 {
            println!("attempting to recycle somethign that was not pending!!");
            println!("post rtrn one: availabe {:064b}", self.available);
            println!("post rtrn one: av. mask {:064b}", self.available_order);
            println!("post rtrn one:  pending {:064b}", self.pending);
            panic!("exiting")
        }
        self.pending ^= pending_bit;
        self.available |= pending_bit;

        // if self.available_order == 0 {
        //     self.available_order = self.all_bits;
        // }

        // println!("post rtrn one: availabe {:064b}", self.available);
        // println!("post rtrn one: av. mask {:064b}", self.available_order);
        // println!("post rtrn one:  pending {:064b}", self.pending);

    } 

    pub(crate) fn take_one(&mut self) -> Option<u32> {
        // println!("pre take one:  availabe {:064b}", self.available);
        // println!("pre take one:  av. mask {:064b}", self.available_order);
        // println!("pre take one:   pending {:064b}", self.pending);

        let masked = self.available & self.available_order;
        if masked == 0 {
            None
        } else {
            let pending_index = self.size - masked.trailing_zeros() - 1;
            println!("take one: pending_index {pending_index}");

            let pending_bit= 1usize << (self.size - pending_index - 1);

            if (self.pending & pending_bit) != 0 {
                println!("attempting to take something that is aalready pending!!");
                println!("post rtrn one: availabe {:064b}", self.available);
                println!("post rtrn one: av. mask {:064b}", self.available_order);
                println!("post rtrn one:  pending {:064b}", self.pending);
                panic!("exiting")
            }

            self.pending |= pending_bit;
            self.available ^= pending_bit;

            self.available_order ^= pending_bit;
            if self.available_order == 0 {
                self.available_order = self.all_bits;
            } 

            // println!("post take one: availabe {:064b}", self.available);
            // println!("post take one: av. mask {:064b}", self.available_order);
            // println!("post take one:  pending {:064b}", self.pending);

            Some(pending_index as u32)
        }

    }

    pub(crate) fn capacity(&self) -> u32 {
        self.size
    }
    pub(crate) fn available(&self) -> u32{
        self.available.count_ones()
    }

    pub(crate) fn get_available_set(&self) -> usize{
        self.available
    }

    pub(crate) fn pending(&self) -> u32 {
        self.pending.count_ones()
    }

    pub(crate) fn get_pending_set(&self) -> usize {
        self.pending
    }

    pub(crate) fn no_pending(&self) -> bool {
        self.pending == 0
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.available == 0
    }

    fn get_window(&self) -> Option<(u32, u32)> {
        
        // println!("pre get_window: masked  {masked:064b}");
        if self.pending == 0 {
            None
        } else {
            let left_most = self.size - (64 - self.pending.leading_zeros());
            // println!("pend: {left_most}");
            let right_most = self.size - self.pending.trailing_zeros() - 1;
            // println!("pend: {right_most}");
            Some((right_most, left_most))
        }
    }

}


pub(crate) struct ConsumerState {
    clear_set: usize,
    all_bits: usize,
    size: u32,
}

impl ConsumerState {
    pub(crate) fn new(size: u32) -> Self {
        let (all_bits, _) = usize::MAX.overflowing_shr(64-size);

        Self { clear_set: 0, all_bits, size }
    }

    pub(crate) fn join(&mut self, buffer_state: &BufferState) {
        // if buffer_state.no_pending() {
        //     self.clear_set = buffer_state.all_bits;
        // } else {
        //     self.clear_set = (buffer_state.all_bits << buffer_state.pending.trailing_zeros()) & buffer_state.all_bits;
        // }

    }

    pub(crate) fn nothing_to_consume(&self, pending: usize) -> bool {
        (pending ^ self.clear_set) == 0
    }

    pub(crate) fn get_clear_set(&self) -> usize {
        self.clear_set
    }

    pub(crate) fn set_clear_set(&mut self, set: usize)  {
        self.clear_set = set
    }

    pub(crate) fn all_cleared(&self) -> bool {
        self.clear_set == self.all_bits
    }

    pub(crate) fn consume(&mut self, pending: usize) -> Option<u32>{
        let size = self.size as usize;

        let masked = pending ^ self.clear_set;
        println!("pre consume:    pending {:0size$b}", pending);
        println!("pre consume:  clear set {:0size$b}", self.clear_set);
        println!("pre consume:     masked {:0size$b}", masked);

        if masked == 0 {
            None
        } else {
            let trailing = (masked << (pending & masked).leading_zeros()).leading_ones();
            let lsb = trailing-1;
            println!("{:?}: consuming: index {lsb}, size {}", std::thread::current().id(), self.size);
            let consumed_bit = 1usize << (self.size - lsb -1);
            // println!("post consume: consume bit {:064b}", consumed_bit);

            self.clear_set ^= consumed_bit;

            println!("post consume:   pending {:0size$b}", pending);
            println!("post consume: clear set {:0size$b}", self.clear_set);

            Some(lsb)
        }
    }



}

#[cfg(test)]
mod tests {
    use crate::{ordering::ConsumerState, consumer};

    use super::BufferState;

    #[ignore = "reason"]
    #[test]
    fn test_size_one() {
        const BIT_COUNT: u32  = 1;

        let mut state = BufferState::new(BIT_COUNT);
        assert!(state.available() == BIT_COUNT);

        assert!(state.take_one() == Some(0)); // take 0
        assert!(state.take_one().is_none());
        state.return_one(0); // return 0
        assert!(state.available() == BIT_COUNT);

    }

    #[ignore = "reason"]
    #[test]
    fn test_wrap() {
        const BIT_COUNT: u32  = 5;

        let mut state = BufferState::new(BIT_COUNT);

        assert!(state.available() == BIT_COUNT);
        assert!(state.pending() == 0);
        assert!(state.get_window().is_none());


        let mut index = state.take_one();
        assert!(index == Some(BIT_COUNT-1));
        assert!(state.available() == BIT_COUNT-1);
        assert!(state.pending() == 1);
        assert!(state.get_window() == Some(( BIT_COUNT-1,  BIT_COUNT-1)));

        index = state.take_one();
        assert!(index == Some(BIT_COUNT-2));
        assert!(state.available() == BIT_COUNT-2);
        assert!(state.pending() == 2);
        assert!(state.get_window() == Some(( BIT_COUNT-1,  BIT_COUNT-2)));

        index = state.take_one();
        assert!(index == Some(BIT_COUNT-3));
        assert!(state.available() == BIT_COUNT-3);
        assert!(state.pending() == 3);
        assert!(state.get_window() == Some(( BIT_COUNT-1,  BIT_COUNT-3)));

        index = state.take_one();
        assert!(index == Some(BIT_COUNT-4));
        assert!(state.available() == BIT_COUNT-4);
        assert!(state.pending() == 4);
        assert!(state.get_window() == Some(( BIT_COUNT-1,  BIT_COUNT-4)));

        index = state.take_one();
        assert!(index == Some(BIT_COUNT-5));
        assert!(state.available() == BIT_COUNT-5);
        assert!(state.pending() == 5);
        assert!(state.get_window() == Some(( BIT_COUNT-1,  BIT_COUNT-5)));

        index = state.take_one();
        assert!(index.is_none());
        assert!(state.available() == 0);
        assert!(state.pending() == 5);
        assert!(state.get_window() == Some(( 4,  0)));

        state.return_one(4);
        assert!(state.available() == 1);
        assert!(state.pending() == 4);
        assert!(state.get_window() == Some(( 3,  0)));

        index = state.take_one();
        assert!(index == Some(4));
        assert!(state.available() == 0);
        assert!(state.pending() == 5);
        assert!(state.get_window() == Some(( 4,  0)));


    }

    #[ignore = "reason"]
    #[test]
    fn test_return() {
        const BIT_COUNT: u32  = 5;

        let mut state = BufferState::new(BIT_COUNT);

        assert!(state.take_one() == Some(4)); // takes index 4
        assert!(state.take_one() == Some(3)); // takes index 3
        assert!(state.pending() == 2);
        assert!(state.available() == 3);

        state.return_one(4); // returns idnex 4
        assert!(state.get_window() == Some(( 3,  3)));
        assert!(state.pending() == 1);
        assert!(state.available() == 4);

        assert!(state.take_one() == Some(2)); // takes index 2
        assert!(state.pending() == 2);
        assert!(state.get_window() == Some(( 3,  2)));

        state.return_one(3); // returns idnex 3
        assert!(state.pending() == 1);
        assert!(state.get_window() == Some(( 2,  2)));


        assert!(state.take_one() == Some(1)); // takes index 1
        assert!(state.pending() == 2);
        assert!(state.get_window() == Some(( 2,  1)));


        state.return_one(2); // returns idnex 2
        assert!(state.pending() == 1);
        assert!(state.get_window() == Some(( 1,  1)));

        state.return_one(1); // returns idnex 1
        assert!(state.take_one() == Some(0)); // takes index 0
        state.return_one(0); // returns idnex 0
        assert!(state.take_one() == Some(4)); // takes index 4
        assert!(state.take_one() == Some(3)); // takes index 3
        assert!(state.take_one() == Some(2)); // takes index 2
        assert!(state.take_one() == Some(1)); // takes index 1
        assert!(state.take_one() == Some(0)); // takes index 0
        state.return_one(4); // returns idnex 4
        state.return_one(3); // returns idnex 3
        state.return_one(2); // returns idnex 2
        state.return_one(1); // returns idnex 1
        state.return_one(0); // returns idnex 0
        assert!(state.take_one() == Some(4)); // takes index 4
        assert!(state.take_one() == Some(3)); // takes index 3
        assert!(state.take_one() == Some(2)); // takes index 2
        assert!(state.take_one() == Some(1)); // takes index 1
        assert!(state.take_one() == Some(0)); // takes index 0

    }

    #[ignore = "reason"]
    #[test]
    fn test_consumer() {
        const BIT_COUNT: u32  = 5;

        let mut state = BufferState::new(BIT_COUNT);

        let mut consumer_1 = ConsumerState::new(BIT_COUNT);
        assert!(consumer_1.consume(state.pending).is_none());

        state.take_one(); // take 4
        assert!(consumer_1.consume(state.pending) == Some(4));
        assert!(consumer_1.consume(state.pending).is_none());

        state.take_one(); // take 3
        assert!(consumer_1.consume(state.pending) == Some(3));
        assert!(consumer_1.consume(state.pending).is_none());

        let mut consumer_2 = ConsumerState::new(BIT_COUNT);
        consumer_2.join(&state);
        assert!(consumer_2.consume(state.pending) == Some(4));
        assert!(consumer_2.consume(state.pending) == Some(3));


        state.take_one(); // take 2
        state.take_one(); // take 1
        assert!(consumer_1.consume(state.pending) == Some(2));
        assert!(consumer_1.consume(state.pending) == Some(1));

        state.return_one(4);   // return 4
        state.take_one(); // take 0
        assert!(consumer_1.consume(state.pending) == Some(0));



    }
}


