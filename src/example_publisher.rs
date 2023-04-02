use shm_broadcast::{BroadcastSegment}

fn main() {
    let out = BroadcastSegment::new("hello", 100000, 8);
    loop {
        out;
        std::thread::sleep(time::Duration::from_millis(10));

    }
}
