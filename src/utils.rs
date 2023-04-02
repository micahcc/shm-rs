use crate::errors::SocketError;

use crc::{Crc, CRC_32_CKSUM};

use libc::socket;
use std::os::fd::{AsRawFd, FromRawFd};
use std::os::unix::net::{UnixListener, UnixStream}; // needed for from_raw_fd

pub fn now_micros() -> u64 {
    return std::time::SystemTime::now()
        .duration_since(std::time::SystemTime::UNIX_EPOCH)
        .expect("Time should be after epoch")
        .as_micros() as u64;
}

pub fn compute_crc32(data: &[u8]) -> u32 {
    let crc = Crc::<u32>::new(&CRC_32_CKSUM);
    let mut digest = crc.digest();
    digest.update(data);
    return digest.finalize();
}

pub fn make_seq_socket_listener(sock_path: &str) -> Result<UnixListener, SocketError> {
    let fd: UnixListener;
    unsafe {
        let raw_fd = socket(libc::AF_UNIX, libc::SOCK_SEQPACKET | libc::SOCK_NONBLOCK, 0);
        if raw_fd < 0 {
            return Err(SocketError::new("Failed to construct socket".to_string()));
        }
        fd = UnixListener::from_raw_fd(raw_fd);
    }

    //name.sun_family = AF_UNIX;
    // strncpy(name.sun_path, SOCKET_NAME, sizeof(name.sun_path) - 1);
    // need 0 at begin, and 0 at end, so only have 12 characters
    if sock_path.len() > 12 {
        return Err(SocketError::new(format!(
            "Socket name should be < 12 characters (got {})",
            sock_path.len()
        )));
    }

    let mut sa_data: [libc::c_char; 14] = [0; 14];
    for (i, c) in sock_path.bytes().enumerate() {
        sa_data[i + 1] = c as i8;
    }
    let addr = libc::sockaddr {
        sa_family: libc::AF_UNIX as u16,
        sa_data: sa_data,
    };

    unsafe {
        let ret = libc::bind(fd.as_raw_fd(), &addr, 16);
        if ret == -1 {
            return Err(SocketError::new(
                format!("Failed to bind to {}", ret).to_string(),
            ));
        }
    }

    unsafe {
        let ret = libc::listen(fd.as_raw_fd(), 1);
        if ret == -1 {
            return Err(SocketError::new(
                format!("Failed to listen to {}", ret).to_string(),
            ));
        }
    }

    return Ok(fd);
}

pub fn make_seq_socket_connection(sock_path: &str) -> Result<UnixStream, std::io::Error> {
    let fd: UnixStream;
    unsafe {
        let raw_fd = socket(libc::AF_UNIX, libc::SOCK_SEQPACKET | libc::SOCK_NONBLOCK, 0);
        if raw_fd < 0 {
            return Err(std::io::Error::last_os_error());
        }
        fd = UnixStream::from_raw_fd(raw_fd);
    }

    //name.sun_family = AF_UNIX;
    // strncpy(name.sun_path, SOCKET_NAME, sizeof(name.sun_path) - 1);
    // need 0 at begin, and 0 at end, so only have 12 characters
    if sock_path.len() > 12 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "Socket name should be < 12 characters",
        ));
    }

    let mut sa_data: [libc::c_char; 14] = [0; 14];
    for (i, c) in sock_path.bytes().enumerate() {
        sa_data[i + 1] = c as i8;
    }

    let addr = libc::sockaddr {
        sa_family: libc::AF_UNIX as u16, // 2 bytes
        sa_data: sa_data,                // 14 bytes
    };

    unsafe {
        let ret = libc::connect(fd.as_raw_fd(), &addr, 16);
        if ret < 0 {
            return Err(std::io::Error::last_os_error());
        }
    }

    return Ok(fd);
}
