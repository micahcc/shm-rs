// socket
// write_segment
use crate::Error;
use crate::WriteInterface;
use crate::WriteSegment;
use log::{error, info, warn};
use sendfd::SendWithFd;
use std::io::Write;
use std::os::fd::AsRawFd;
use std::os::fd::OwnedFd;
use std::os::unix::net::{UnixListener, UnixStream};
use std::sync::{Arc, Mutex};

pub struct BroadcastState {
    // socket
    listener: UnixListener,
    clients: Vec<UnixStream>,

    // segment fd is sent to all new connections
    segment_fd: OwnedFd,
}

pub struct BroadcastSegment {
    // write segment
    segment: WriteSegment,
    state: Arc<Mutex<BroadcastState>>,
}

pub struct BroadcastTicker {
    state: Arc<Mutex<BroadcastState>>,
}

impl BroadcastTicker {
    pub fn tick(&mut self) {
        let mut state = self.state.lock().unwrap();
        loop {
            let stream = match state.listener.incoming().next() {
                Some(Ok(stream)) => stream,
                Some(Err(err)) => match err.kind() {
                    std::io::ErrorKind::WouldBlock => return,
                    _ => {
                        error!("Connection failed: {}", err);
                        return;
                    }
                },
                None => return,
            };

            let bytes = [0; 1];
            let fds = [state.segment_fd.as_raw_fd(); 1];
            stream
                .send_with_fd(&bytes, &fds)
                .expect("Somehow brand new socket isn't available");

            state.clients.push(stream);
        }
    }
}

impl BroadcastSegment {
    pub fn new(
        socket_path: &str,
        n_bytes: usize,
        n_messages: u32,
    ) -> Result<BroadcastSegment, Error> {
        let listener = UnixListener::bind(socket_path)?;
        listener
            .set_nonblocking(true)
            .expect("Couldn't set non blocking");

        let segment = WriteSegment::new(socket_path, n_bytes, n_messages)?;
        return Ok(BroadcastSegment {
            state: Arc::new(Mutex::new(BroadcastState {
                listener: listener,
                clients: vec![],
                segment_fd: segment.to_owned_fd(),
            })),
            segment: segment,
        });
    }

    pub fn get_write_buffer(&mut self) -> Result<WriteInterface, Error> {
        return self.segment.get_write_buffer();
    }

    pub fn complete_write(&self, buffer: WriteInterface) {
        buffer.complete_write();

        // notify readers
        let mut state = self.state.lock().unwrap();
        state.clients.retain_mut(|client| {
            match client.write(&[0; 1]) {
                Ok(n_bytes) => {
                    assert!(n_bytes == 1);
                    return true;
                }
                Err(err) => match err.kind() {
                    std::io::ErrorKind::WouldBlock => {
                        // this is ok, just means the client has fallend
                        warn!("Client: {:?}, has fallen behind", client);
                        return true;
                    }
                    _ => {
                        info!("Connection closed: {}", err);
                        return false;
                    }
                },
            }
        });
    }

    pub fn make_ticker(&mut self) -> BroadcastTicker {
        return BroadcastTicker {
            state: self.state.clone(),
        };
    }
}
