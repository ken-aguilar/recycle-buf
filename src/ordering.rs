
pub(crate) struct BufferState {

    available: usize,
    available_order: usize,
    pending: usize,

    all_bits: usize,
    size: u32

}

impl BufferState {
    pub fn new(size: u32) -> Self {
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

    pub fn return_one(&mut self, index: u32) {
        let pending_bit = 1usize << (self.size - index -1);
        self.pending ^= pending_bit;
        self.available |= pending_bit;

        if self.available_order == 0 {
            self.available_order = self.all_bits;
        }

        // println!("post rtrn one: availabe {:064b}", self.available);
        // println!("post rtrn one: av. mask {:064b}", self.available_order);
        // println!("post rtrn one:  pending {:064b}", self.pending);

    } 

    fn take_one(&mut self) -> Option<u32> {
        // println!("pre take one:  availabe {:064b}", self.available);
        // println!("pre take one:  av. mask {:064b}", self.available_order);
        // println!("pre take one:   pending {:064b}", self.pending);

        let masked = self.available & self.available_order;
        if masked == 0 {
            None
        } else {
            let pending_index = self.size - masked.trailing_zeros() - 1;
            // println!("take one: pending_index {pending_index}");

            let pending_bit= 1usize << (self.size - pending_index - 1);
            self.pending |= pending_bit;
            self.available ^= pending_bit;

            if self.available_order == 0 {
                self.available_order = self.all_bits;
            } else {
                self.available_order ^= pending_bit;
            }

            // println!("post take one: availabe {:064b}", self.available);
            // println!("post take one: av. mask {:064b}", self.available_order);
            // println!("post take one:  pending {:064b}", self.pending);

            Some(pending_index as u32)
        }

    }

    fn capacity(&self) -> u32 {
        self.size
    }
    fn available(&self) -> u32{
        self.available.count_ones()
    }

    fn pending(&self) -> u32 {
        self.pending.count_ones()
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
    consume_order: usize,
}

impl ConsumerState {
    fn new(size: u32) -> Self {
        let (all_bits, _) = usize::MAX.overflowing_shr(64-size);

        Self { consume_order: all_bits }
    }

    fn join(&mut self, buffer_state: &BufferState) {
        self.consume_order = (buffer_state.all_bits << buffer_state.pending.trailing_zeros()) & buffer_state.all_bits;

    }
    fn consume(&mut self, buffer_state: &mut BufferState) -> Option<u32>{
        let masked = buffer_state.pending & self.consume_order;

        if masked == 0 {
            None
        } else {
            let trailing = masked.trailing_zeros();
            let lsb = buffer_state.size - trailing - 1;
            let consumed_bit = 1usize << trailing;
            if self.consume_order == 0 {
                self.consume_order = buffer_state.all_bits;
            } else {
                self.consume_order ^= consumed_bit;
            }
            println!("post consume:   pending {:064b}", buffer_state.pending);
            println!("post consume: cosum ord {:064b}", self.consume_order);

            Some(lsb)
        }
    }



}

#[cfg(test)]
mod tests {
    use crate::{ordering::ConsumerState, consumer};

    use super::BufferState;


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

    #[test]
    fn test_consumer() {
        const BIT_COUNT: u32  = 5;

        let mut state = BufferState::new(BIT_COUNT);

        let mut consumer_1 = ConsumerState::new(BIT_COUNT);
        assert!(consumer_1.consume(&mut state).is_none());

        state.take_one(); // take 4
        assert!(consumer_1.consume(&mut state) == Some(4));
        assert!(consumer_1.consume(&mut state).is_none());

        state.take_one(); // take 3
        assert!(consumer_1.consume(&mut state) == Some(3));
        assert!(consumer_1.consume(&mut state).is_none());

        let mut consumer_2 = ConsumerState::new(BIT_COUNT);
        consumer_2.join(&state);
        assert!(consumer_2.consume(&mut state) == Some(4));
        assert!(consumer_2.consume(&mut state) == Some(3));


        state.take_one(); // take 2
        state.take_one(); // take 1
        assert!(consumer_1.consume(&mut state) == Some(2));
        assert!(consumer_1.consume(&mut state) == Some(1));

        state.return_one(4);   // return 4
        state.take_one(); // take 0
        assert!(consumer_1.consume(&mut state) == Some(0));

    }
}


