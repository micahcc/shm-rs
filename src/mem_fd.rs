use crate::errors::SocketError;
use std::ffi::CString;
use std::os::fd::AsRawFd;
use std::os::fd::FromRawFd;
use std::os::fd::OwnedFd;

pub struct MemFd {
    file_fd: OwnedFd,
    addr: *mut u8,
    n_bytes: usize,
}

impl MemFd {
    // TODO(micah) could probably use generics...
    pub fn read_u32_at(&self, start: usize) -> u32 {
        let slice = self.slice();
        let end: usize = start + 4;
        assert!(end < slice.len());
        let data: &[u8; 4] = &slice[start..end].try_into().unwrap();
        return u32::from_ne_bytes(*data);
    }

    pub fn read_u8_at(&self, pos: usize) -> u8 {
        let slice = self.slice();
        assert!(pos < slice.len());
        return slice[pos];
    }

    pub fn read_u64_at(&self, start: usize) -> u64 {
        let slice = self.slice();
        let end: usize = start + 8;
        assert!(end < slice.len());
        let data: &[u8; 8] = &slice[start..end].try_into().unwrap();
        return u64::from_ne_bytes(*data);
    }

    pub fn write_u64_at(&mut self, start: usize, value: u64) {
        let slice = self.mut_slice();
        let end: usize = start + 8;
        let arr = value.to_ne_bytes();
        assert!(end < slice.len());
        slice[start..end].clone_from_slice(&arr);
    }

    pub fn write_u32_at(&mut self, start: usize, value: u32) {
        let slice = self.mut_slice();
        let end: usize = start + 4;
        assert!(end < slice.len());
        let arr = value.to_ne_bytes();
        slice[start..end].clone_from_slice(&arr);
    }

    pub fn write_u8_at(&mut self, pos: usize, value: u8) {
        let slice = self.mut_slice();
        assert!(pos < slice.len());
        slice[pos] = value;
    }

    pub unsafe fn ptr_to(&mut self, start: usize) -> *mut u8 {
        return self.addr.offset(start as isize);
    }

    pub fn from_owned_fd(file_fd: OwnedFd) -> Result<MemFd, SocketError> {
        //let memory_overhead = HEADER_SIZE + MESSAGE_HEADER_SIZE * (n_messages as usize);
        //let max_message_size = (n_bytes - memory_overhead) / (n_messages as usize);

        unsafe {
            // seek to end, find length
            let n_bytes = libc::lseek(file_fd.as_raw_fd(), 0, libc::SEEK_END);
            if n_bytes < 0 {
                return Err(SocketError::new(format!(
                    "Failed to seed to end: {}",
                    std::io::Error::last_os_error()
                )));
            }

            let hint: *mut libc::c_void = std::ptr::null_mut();
            let addr = libc::mmap64(
                hint,
                n_bytes as usize,
                libc::PROT_READ,
                libc::MAP_SHARED, // need to get back to underlying fd
                file_fd.as_raw_fd(),
                0,
            );

            if addr as i64 <= 0 {
                return Err(SocketError::new(format!(
                    "Failed to map memory: {}",
                    std::io::Error::last_os_error()
                )));
            }

            return Ok(MemFd {
                addr: addr as *mut u8,
                file_fd: file_fd,
                n_bytes: n_bytes as usize,
            });
        }
    }

    pub fn new(name: &str, n_bytes: usize) -> Result<MemFd, SocketError> {
        unsafe {
            let topic_c_str = CString::new(name).unwrap();
            let raw_file_fd = libc::memfd_create(topic_c_str.into_raw(), 0);
            if raw_file_fd < 0 {
                return Err(SocketError::new(format!(
                    "Failed to construct memfd: {}",
                    std::io::Error::last_os_error()
                )));
            }
            let file_fd = OwnedFd::from_raw_fd(raw_file_fd);

            if libc::ftruncate(file_fd.as_raw_fd(), n_bytes as i64) < 0 {
                return Err(SocketError::new(format!(
                    "Failed to resize memfd: {}",
                    std::io::Error::last_os_error()
                )));
            }

            let hint: *mut libc::c_void = std::ptr::null_mut();
            let addr = libc::mmap64(
                hint,
                n_bytes as usize,
                libc::PROT_READ | libc::PROT_WRITE,
                libc::MAP_SHARED, // need to read from underlying file
                file_fd.as_raw_fd(),
                0,
            );

            if addr as i64 <= 0 {
                return Err(SocketError::new(format!(
                    "Failed to map memory: {}",
                    std::io::Error::last_os_error()
                )));
            }

            assert!(!addr.is_null());
            return Ok(MemFd {
                addr: addr as *mut u8,
                file_fd: file_fd,
                n_bytes: n_bytes,
            });
        }
    }

    pub fn len(&self) -> usize {
        return self.n_bytes;
    }

    pub fn slice(&self) -> &mut [u8] {
        assert!(!self.addr.is_null());
        let slice = unsafe { std::slice::from_raw_parts_mut(self.addr, self.n_bytes as usize) };
        return slice;
    }

    pub fn mut_slice(&mut self) -> &mut [u8] {
        assert!(!self.addr.is_null());
        let slice = unsafe { std::slice::from_raw_parts_mut(self.addr, self.n_bytes as usize) };
        return slice;
    }

    pub fn to_owned_fd(&self) -> OwnedFd {
        return self.file_fd.try_clone().expect("Failed to clone");
    }
}

impl Drop for MemFd {
    fn drop(&mut self) {
        unsafe {
            libc::munmap(self.addr as *mut libc::c_void, self.n_bytes);
        }
    }
}
