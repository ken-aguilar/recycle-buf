# recycle-buf
A thread safe, memory recycling system written in rust. This system allows pre-allocation of objects in a buffer and facilitates their reuse upon being dropped. 

The buffer can be created using the builder pattern

struct CameraFrame {
  dimensions: (u32, u32),
  pixels: Box<[u8]>
}

impl CameraFrame {
  
  fn new(dimensions: (u32, u32)) -> CameraFrame {
  }
}


fn main {
  let mut recycler = RecyclerBuilder::new()
            .push(CameraFrame::new((1024, 1024)))
            .push(CameraFrame::new((1024, 1024)))
            .build();
            
  
}
