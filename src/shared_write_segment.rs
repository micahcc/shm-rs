/*
 * Implements a single writer, multiple reader shared slab of memory.
 * There are a couple of complications though:
 * 1. we need to have shared sequnces for headers and body
 * 2. It would be nice not to tie users down with first allocating a chunk, then
      unevently distributing that between the header and body (or assuming some ratio)
*/
use crate::constants::{
    MESSAGE_HEADER_SIZE, MSG_POS_CRC, MSG_POS_OFFSET, MSG_POS_SEQ, MSG_POS_SIZE, MSG_POS_TIMESTAMP,
    PRIMARY_HEADER_SIZE,
};
use crate::error::Error;
use crate::mem_fd::MemFd;
use crate::utils::{compute_crc32, now_micros};
use log::{error, info, warn};

// We may need to share the same write memory between many users, so keep a lock on the
// actual memory and store it in SharedWriteSegment
pub struct SharedWriteSegment {
    pub mem_fd: MemFd,

    // where to write next in memory
    // every time we call allocate we advance this forward and invalidate
    // any headers for messages that we need to overwrite
    pub write_position: usize,

    pub n_messages: u32,
}

fn overlaps(pos1: usize, len1: usize, pos2: usize, len2: usize) -> bool {
    // trick here is check if the max of one is less than the min of the other
    // then check vice versa
    // max of section 1 is less than min of section 2
    // max of section2 is less than min of section 1
    if pos1 + len1 <= pos2 || pos2 + len2 <= pos1 {
        return false;
    } else {
        return true;
    }
}

impl SharedWriteSegment {
    pub fn new(name: &str, n_bytes: usize, n_messages: u32) -> Result<SharedWriteSegment, Error> {
        let mut mem_fd = MemFd::new(name, n_bytes)?;
        let write_position = n_messages as usize * MESSAGE_HEADER_SIZE + PRIMARY_HEADER_SIZE;

        // initialize sequences to 1
        for i in 0..n_messages {
            let start: usize = PRIMARY_HEADER_SIZE + MESSAGE_HEADER_SIZE * (i as usize);
            // write seq = 1 (ok to overwrite)
            mem_fd.write_u64_at(start + MSG_POS_SEQ, 1); // seq (1 is ok to overwrite)
            mem_fd.write_u64_at(start + MSG_POS_TIMESTAMP, 0); // timestamp
            mem_fd.write_u32_at(start + MSG_POS_OFFSET, 0); // offset
            mem_fd.write_u32_at(start + MSG_POS_SIZE, 0); // size
            mem_fd.write_u32_at(start + MSG_POS_CRC, 0); // crc
        }

        return Ok(SharedWriteSegment {
            mem_fd: mem_fd,
            write_position: write_position,
            n_messages: n_messages,
        });
    }

    pub fn start_write(&mut self) -> Result<usize, Error> {
        // grab the lowest sequence > 0 and invalidate it
        // we should be the only writers so it is safe to scan through to find the next
        // sequence without checking for races
        let mut min_index = u32::MAX;
        let mut min_seq = u64::MAX;
        for i in 0..self.n_messages {
            let seq = self.mem_fd.read_u64_at(
                PRIMARY_HEADER_SIZE + MESSAGE_HEADER_SIZE * (i as usize) + MSG_POS_SEQ,
            );
            if seq > 0 && seq < min_seq {
                min_seq = seq;
                min_index = i;
            }
        }

        if min_seq == u64::MAX {
            // if everything was zero (in flight) this could happen
            return Err(Error::new(format!(
                "Too many simultaneous buffers being written, could not allocate"
            )));
        }

        // invalidate (zero) the sequence to indicate we're using it
        // the invalidate the rest of the header
        let start = PRIMARY_HEADER_SIZE + MESSAGE_HEADER_SIZE * (min_index as usize);
        self.mem_fd.write_u64_at(start + MSG_POS_SEQ, 0);

        self.mem_fd.write_u64_at(start + MSG_POS_TIMESTAMP, 0); // sent timestamp
        self.mem_fd.write_u32_at(start + MSG_POS_OFFSET, 0); // offset
        self.mem_fd.write_u32_at(start + MSG_POS_SIZE, 0); // size
        self.mem_fd.write_u32_at(start + MSG_POS_CRC, 0); // crc32

        return Ok(start);
    }

    pub fn complete_write(&mut self, meta_position: usize, seq: u64) {
        let read_seq = self.mem_fd.read_u64_at(meta_position + 0);
        if read_seq != 0 {
            error!(
                "Something is wrong at {}, the sequence should have been zero \
                but is {}. This could indicate complete_write() being called \
                twice on the same data or more general corruption",
                meta_position, read_seq
            );
            return;
        }

        // read the product of any work (head + size)
        let offset = self.mem_fd.read_u32_at(meta_position + MSG_POS_OFFSET) as usize;
        let size = self.mem_fd.read_u32_at(meta_position + MSG_POS_SIZE) as usize;

        // compute and write CRC32
        let end = offset + size;
        let crc = compute_crc32(&self.mem_fd.slice()[offset..end]);
        self.mem_fd.write_u32_at(meta_position + MSG_POS_CRC, crc);

        // write timestamp
        self.mem_fd
            .write_u64_at(meta_position + MSG_POS_TIMESTAMP, now_micros());

        // Finally write sequence, this indicates to readers that we're done.
        // compiler fence is overkill, but the idea is to ensure that the
        // following write doesn't get moved above here
        std::sync::atomic::compiler_fence(std::sync::atomic::Ordering::Release);
        self.mem_fd.write_u64_at(meta_position + MSG_POS_SEQ, seq);
    }

    pub fn allocate_block(
        &mut self,
        alloc_meta_pos: usize,
        alloc_len: usize,
    ) -> Result<*mut u8, Error> {
        if alloc_len > self.mem_fd.len() {
            return Err(Error::new(format!(
                "Attemped to allocate {}, but we only have {} bytes",
                alloc_len,
                self.mem_fd.len()
            )));
        }

        // NOTE: we're the only writer of this block at this time, so some stuff
        // that might seem racey is actually not
        // NOTE: WE DO NEED TO INVALIDATE SEQUENCES BEFORE CHANGING OTHER META
        // General algorithm is to start from current write head and overwrite
        // anything in the way.
        // To do that, we'll remove all old messages up to the maximum sequence
        // that we overlap with. This prevents holes because lower sequnces are
        // invalidated rather than keeping them.

        // if the write position is too late in the buffer to be used start over
        // at 0
        if self.write_position + alloc_len > self.mem_fd.len() {
            // at the end, must wrap
            self.write_position =
                PRIMARY_HEADER_SIZE + MESSAGE_HEADER_SIZE * self.n_messages as usize;
        }

        // starting at write position, invalidate any headers that overlap
        // find max segment that overlaps
        let mut max_del_seq = 1;
        let start = self.write_position;
        for i in 0..self.n_messages {
            let meta_pos = PRIMARY_HEADER_SIZE + MESSAGE_HEADER_SIZE * (i as usize);
            let read_seq = self.mem_fd.read_u64_at(meta_pos);
            let head_offset = self.mem_fd.read_u32_at(meta_pos + 16) as usize;
            let head_size = self.mem_fd.read_u32_at(meta_pos + 20) as usize;
            let body_offset = self.mem_fd.read_u32_at(meta_pos + 28) as usize;
            let body_size = self.mem_fd.read_u32_at(meta_pos + 32) as usize;

            if overlaps(head_offset, head_size, self.write_position, alloc_len)
                || overlaps(body_offset, body_size, self.write_position, alloc_len)
            {
                // NOTE: if we hit a sequence of zero that overlaps, then we are in trouble,
                // because that means theres another array in flight that we can't
                // overwrite. In that case we must abort because it means that
                // we've wrapped all the way around to another in flight message.
                if read_seq == 0 {
                    return Err(Error::new(format!(
                        "Attempting to overwrite another sequence \
                    that is in flight, this is usually because you are allocating very \
                    large messages relative to the total shared memory or you have many \
                    messages inflight at once, maybe do fewer simultaneous writes?",
                    )));
                }
                max_del_seq = u64::max(max_del_seq, read_seq);
            }
        }

        // invalidate all up to the max sequence we're deleting, this is to ensure
        // that we don't leave gaps in the sequence (this could happen if there
        // was a small message at the end of the buffer that we didn't run into
        // because we had to wrap around to the beginning of the buffer)
        // Note that we're not writing anthying but the sequence here, this leave
        // old data in place, but since the sequence is invalid no reader should
        // read it.
        for i in 0..self.n_messages {
            let meta_pos = PRIMARY_HEADER_SIZE + MESSAGE_HEADER_SIZE * (i as usize);
            let read_seq = self.mem_fd.read_u64_at(meta_pos);
            if read_seq > 0 && read_seq <= max_del_seq {
                // write seq = 1 (ok to overwrite)
                self.mem_fd.write_u64_at(meta_pos + MSG_POS_SEQ, 1);
            }
        }

        // ok we have now created enough memory that we can write out our own
        // updated size
        self.mem_fd
            .write_u32_at(alloc_meta_pos + MSG_POS_OFFSET, start as u32);
        self.mem_fd
            .write_u32_at(alloc_meta_pos + MSG_POS_SIZE, alloc_len as u32);

        // advance write position beyond this, this means that this will be the
        // last place we overwrite (which is good because we can't overwrite it
        // currerntly, because the seq is 0)
        self.write_position += alloc_len;

        // return pointer to the memory we have now allocated
        assert!(start < self.mem_fd.len());
        unsafe {
            return Ok(self.mem_fd.ptr_to(start));
        }
    }
}
