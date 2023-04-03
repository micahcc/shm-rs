use crate::read_segment::ReadSegment;
use crate::Error;
use log::{error, warn};
use sendfd::RecvWithFd;
use std::io::prelude::*;
use std::os::fd::FromRawFd;
use std::os::fd::OwnedFd;
use std::os::unix::net::UnixStream;

pub struct SubscribeSegment {
    stream: UnixStream,
    segment: Option<ReadSegment>,
    ready: bool,
    connected: bool,
    stream_timeout: Option<std::time::Duration>,
    latch: bool,
}

impl SubscribeSegment {
    pub fn new(socket_path: &str, latch: bool) -> Result<SubscribeSegment, Error> {
        // connect to socket
        let stream = UnixStream::connect(socket_path)?;
        stream.set_nonblocking(true)?;

        return Ok(SubscribeSegment {
            stream,
            latch,
            segment: None, // should receive this soon
            ready: false,
            connected: true,
            stream_timeout: None,
        });
    }

    pub fn is_connected(&self) -> bool {
        return self.connected;
    }

    pub fn next(&mut self, timeout: Option<std::time::Duration>) -> Option<Vec<u8>> {
        if timeout != self.stream_timeout {
            self.stream
                .set_read_timeout(timeout)
                .expect("Set timeout should succeed");
            self.stream_timeout = timeout;
        }

        if self.connected && self.ready {
            // just read
            return self.read_next_from_segment();
        }

        let mut message_bytes = [0; 1];
        let mut message_fds = [-1; 1];
        match self
            .stream
            .recv_with_fd(&mut message_bytes, &mut message_fds)
        {
            Ok((n_bytes, n_fds)) => {
                if n_bytes == 0 {
                    // zero bytes without error means closed
                    self.connected = false;
                    return None;
                } else if n_fds == 1 && message_fds[0] != -1 {
                    // fd means we have a segment to read
                    if self.segment.is_some() {
                        warn!("Received duplicate segments");
                    }
                    match ReadSegment::new(
                        unsafe { OwnedFd::from_raw_fd(message_fds[0]) },
                        self.latch,
                    ) {
                        Ok(s) => {
                            self.segment = Some(s);
                        }
                        Err(err) => {
                            error!("Failed to read segment from socket: {}", err);
                            return None;
                        }
                    };
                } else {
                    assert!(n_bytes == 1);
                }
            }
            Err(err) => match err.kind() {
                std::io::ErrorKind::WouldBlock => return None,
                _ => {
                    warn!("Connection failed: {}", err);
                    self.connected = false;
                    return None;
                }
            },
        }

        self.ready = true;
        return self.read_next_from_segment();
    }

    fn read_next_from_segment(&self) -> Option<Vec<u8>> {
        todo!();
    }
}
