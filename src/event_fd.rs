use crate::errors::SocketError;
use std::os::fd::AsRawFd;
use std::os::fd::FromRawFd;
use std::os::fd::OwnedFd;
use std::os::fd::RawFd;

pub struct EventFd {
    fd: OwnedFd,
}

impl EventFd {
    pub fn from_owned_fd(fd: OwnedFd) -> EventFd {
        return EventFd { fd: fd };
    }

    pub fn clone(&self) -> EventFd {
        return EventFd {
            fd: self.fd.try_clone().expect("Failed to clone"),
        };
    }

    pub fn new() -> Result<EventFd, SocketError> {
        let fd = unsafe { libc::eventfd(0, libc::EFD_NONBLOCK) };
        if fd == -1 {
            return Err(SocketError::new(format!(
                "Failed to construct eventfd: {}",
                std::io::Error::last_os_error()
            )));
        }
        return Ok(EventFd {
            fd: unsafe { OwnedFd::from_raw_fd(fd) },
        });
    }

    pub fn incr(&self) -> Result<(), SocketError> {
        let value: u64 = 1;
        let ptr: *const u64 = &value;
        let ret;
        unsafe {
            ret = libc::write(self.fd.as_raw_fd(), ptr as *const libc::c_void, 8);
        }

        if ret == -1 {
            return Err(SocketError::new(format!(
                "Failed to decr event: {}",
                std::io::Error::last_os_error()
            )));
        }

        return Ok(());
    }

    pub fn as_raw_fd(&self) -> RawFd {
        return self.fd.as_raw_fd();
    }

    pub fn decr(&self) -> Result<u64, SocketError> {
        let mut value: u64 = 0;
        let ptr: *mut u64 = &mut value;
        let ret;
        unsafe {
            ret = libc::read(self.fd.as_raw_fd(), ptr as *mut libc::c_void, 8);
        }
        if ret == -1 {
            return Err(SocketError::new(format!(
                "Failed to decr event: {}",
                std::io::Error::last_os_error()
            )));
        }

        return Ok(value);
    }

    pub fn to_owned_fd(self) -> OwnedFd {
        return self.fd;
    }
}
