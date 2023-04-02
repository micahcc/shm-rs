use std::os::fd::OwnedFd;

pub struct WireMessage {
    pub data: Vec<u8>,
    // [shm_fd, event_fd]
    pub fds: Vec<OwnedFd>,
}
