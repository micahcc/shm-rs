// socket
// write_segment
use crate::Error;
use crate::WriteInterface;
use crate::WriteSegment;
use std::os::unix::net::UnixListener;

struct BroadcastSegment {
    // socket
    listener: UnixListener,

    // write segment
    segment: WriteSegment,
}

impl BroadcastSegment {
    pub fn new(
        socket_path: &str,
        n_bytes: usize,
        n_messages: u32,
    ) -> Result<BroadcastSegment, Error> {
        let listener = UnixListener::bind(socket_path)?;
        let segment = WriteSegment::new(socket_path, n_bytes, n_messages)?;
        return Ok(BroadcastSegment {
            listener: listener,
            segment: segment,
        });
    }

    pub fn get_write_buffer(&mut self) -> Result<WriteInterface, Error> {
        return self.segment.get_write_buffer();
    }
}
