use shm_broadcast::BroadcastSegment;
use std::time::Duration;

fn main() {
    let mut out = BroadcastSegment::new("hello", 100000, 8).expect("Create broadcaster");
    let message = b"hello";
    loop {
        let mut buffer = out.get_write_buffer().expect("Should have a free buffer");
        let slice = buffer.alloc_slice(message.len()).expect("Slice should fit");
        for i in 0..message.len() {
            slice[i] = message[i];
        }
        buffer.complete_write();

        std::thread::sleep(Duration::from_millis(10));
    }
}
