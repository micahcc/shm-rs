/*
 * Implements a single writer, multiple reader shared slab of memory.
 * There are a couple of complications though:
 * 1. we need to have shared sequnces for headers and body
 * 2. It would be nice not to tie users down with first allocating a chunk, then
      unevently distributing that between the header and body (or assuming some ratio)
*/
use crate::constants::{
    ABS_POS_N_MESSAGES, ABS_POS_SEGMENT_UID, MESSAGE_HEADER_SIZE, MSG_POS_CRC, MSG_POS_OFFSET,
    MSG_POS_SEQ, MSG_POS_SIZE, MSG_POS_TIMESTAMP, PRIMARY_HEADER_SIZE,
};
use crate::error::Error;
use crate::shared_write_segment::SharedWriteSegment;
use log::{error, info, warn};
use std::sync::{Arc, Mutex};

pub struct WriteInterface {
    // where our header is located
    // u64  0 -- seq
    // u64  8 -- send_timestamp
    // u32  16 -- head_offset
    // u32  20 -- head_size
    // u32  24 -- head_crc
    // u32  28 -- body_offset
    // u32  32 -- body_size
    // u32  36 -- body_crc
    //      40 -- end
    pub meta_position: usize,

    // reserved sequence that we will write when we're done
    pub seq: u64,

    // cached so we don't have to lock to get it
    pub segment_uid: u64,

    // memory we'll write to, we want to keep this entire block alive while write
    // buffer is alive (because we have an unsafe pointer)
    pub pos_segment: Arc<Mutex<SharedWriteSegment>>,
}

impl Drop for WriteInterface {
    /// Ensures that any memory associated with the WriteInterface was actually
    /// published.
    fn drop(&mut self) {
        let mut seg = self.pos_segment.lock().unwrap();
        let read_seq = seg.mem_fd.read_u64_at(self.meta_position + MSG_POS_SEQ);
        if read_seq == 0 {
            // this went unpublished
            warn!("Sequence {} was never published", self.seq);
            // write it back to 1, soit can be reclaimed
            seg.mem_fd.write_u64_at(self.meta_position, 1);
        } else {
            assert!(read_seq == self.seq);
        }
    }
}

impl std::fmt::Debug for WriteInterface {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WriteInterface")
            .field("meta_position", &self.meta_position)
            .field("seq", &self.seq)
            .finish()
    }
}

// Write buffer is passed to the user application code giving them the ability
// to write to a segment.
impl WriteInterface {
    pub fn get_seq(&self) -> u64 {
        return self.seq;
    }

    pub fn get_segment_uid(&self) -> u64 {
        return self.segment_uid;
    }

    pub fn alloc_slice(&mut self, len: usize) -> Result<&mut [u8], Error> {
        // our own book keeping of sub-regions keeps this safe (won't write if
        // value is 0)
        // this escapes the mem_fd lock, but thats ok because
        // - we wrote 0 into the sequence and invalidated the CRC
        // - nothing should write to a region with a sequence of 0.
        // - nothing should read from a region with a sequence of 0
        let mut seg = self.pos_segment.lock().unwrap();
        let ptr = seg.allocate_block(self.meta_position, false, len)?;
        unsafe {
            return Ok(std::slice::from_raw_parts_mut(ptr, len));
        }
    }

    pub fn complete_write(self) {
        let mut seg = self.pos_segment.lock().unwrap();
        seg.complete_write(self.meta_position, self.seq);
    }
}
