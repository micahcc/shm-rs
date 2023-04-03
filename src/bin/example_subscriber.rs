use log::info;
use shm_broadcast::SubscribeSegment;
use std::time::Duration;

fn main() {
    let mut sub = SubscribeSegment::new("hello", true).expect("Create subscriber");
    while sub.is_connected() {
        match sub.next(Some(Duration::new(1, 0))) {
            Some(msg) => {
                info!("{:?}", msg);
            }
            None => {
                info!("Timeout");
            }
        }
    }
}
