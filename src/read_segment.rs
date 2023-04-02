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
use crate::mem_fd::MemFd;
use crate::utils::compute_crc32;
use log::{debug, info};
use std::os::fd::OwnedFd;
use std::sync::{Arc, Mutex};

pub struct Inbound {
    pub seq: u64,
    pub body: Vec<u8>,
    pub publish_micros: u64,
}

enum ReadResult {
    Nothing,
    Retry,
    Success(u64, u64), // seq, time
}

// We may need to share this read megment between multiple clients
// so its kept in a locked region of the real ReadSegment(s),
// also each ReadSegment might be at a different place in thise memory
struct UniqueReadSegment {
    mem_fd: MemFd,
    n_messages: u32,
}

impl UniqueReadSegment {
    fn new(fd: OwnedFd) -> Result<UniqueReadSegment, Error> {
        let mem_fd = MemFd::from_owned_fd(fd)?;

        // read header
        let n_messages = mem_fd.read_u32_at(8);
        return Ok(UniqueReadSegment {
            mem_fd: mem_fd,
            n_messages: n_messages,
        });
    }

    fn read_message(&self, desired_seq: u64, out: &mut Vec<u8>) -> ReadResult {
        info!("Looking for {}", desired_seq);
        let mut best_seq = u64::MAX; // lowest seq >= desired_seq
        let mut best_index: u32 = u32::MAX;
        for i in 0..self.n_messages {
            let pos = PRIMARY_HEADER_SIZE + MESSAGE_HEADER_SIZE * (i as usize);
            let read_seq = self.mem_fd.read_u64_at(pos + MSG_POS_SEQ);
            if read_seq >= desired_seq && read_seq < best_seq {
                best_seq = read_seq;
                best_index = i;
            }
        }

        if best_seq == u64::MAX || best_index == u32::MAX {
            assert!(best_seq == u64::MAX && best_index == u32::MAX);
            // all sequences lower than desired seq, nothing to read
            return ReadResult::Nothing;
        }

        info!("Best: {}", best_seq);
        let pos = PRIMARY_HEADER_SIZE + MESSAGE_HEADER_SIZE * (best_index as usize);
        let publish_micros = self.mem_fd.read_u64_at(pos + MSG_POS_TIMESTAMP);
        let offset = self.mem_fd.read_u32_at(pos + MSG_POS_OFFSET) as usize;
        let size = self.mem_fd.read_u32_at(pos + MSG_POS_SIZE) as usize;
        let crc = self.mem_fd.read_u32_at(pos + MSG_POS_CRC);

        let end = offset + size;
        if end > self.mem_fd.len() as usize {
            // out of bound, sequence must have been overwritten
            debug!(
                "Extremely rare, received offset: {}, size: {}, \
                    buffer size: {}, this would lead to out of bounds, skipping and \
                    trying again",
                offset,
                size,
                self.mem_fd.len()
            );
            return ReadResult::Retry;
        }

        // ready to our output vectors
        let slice = self.mem_fd.slice();
        out.resize(size, 0);
        out.copy_from_slice(&slice[offset..end]);

        // check sequence again to ensure nothing changed
        // compiler fence is overkill but *potentially* necessary to ensure our
        // final seq read occurs after our copy out. I have no idea if
        // a large copy like that woud be moved by the compiler :shrug:
        std::sync::atomic::compiler_fence(std::sync::atomic::Ordering::SeqCst);

        // CRC32 below can be removed once stability is reached
        let re_seq = self.mem_fd.read_u64_at(pos);
        let re_crc = compute_crc32(&out);
        if re_seq == best_seq && re_crc == crc {
            // success!
            return ReadResult::Success(re_seq, publish_micros);
        }

        // sequence changed while we were reading, redo the whole thing
        // again
        debug!("Stuff changed, try again");
        return ReadResult::Retry;
    }
}

pub struct ReadSegment {
    pub segment_uid: u64,

    pos_segment: Arc<Mutex<UniqueReadSegment>>,

    // what sequence to read next
    read_seq: u64,
}

impl ReadSegment {
    /// Returns message with sequence matching desired_seq OR the next lowest
    /// that is >= desired_seq. This way if you are on seq 13, but while you
    /// were doing other stuff 13 dropped and we're all the way up to 100-110
    /// you don't have to call this 90 times, just call it 13 and we'll return
    /// 100.
    pub fn read_message(&mut self) -> Option<Inbound> {
        // 0, 1 are sentinals
        assert!(
            self.read_seq > 1,
            "0 and 1 are sentinals, you must never try to read them"
        );

        // this is kind of gross, probably the most expensive operation in here
        // will be the allocation, so keep the same vectors through mutliple attempts
        let mut body: Vec<u8> = vec![];

        // loop because we might read and then discover that the data has changed
        // mid read
        let prev_seq = self.read_seq;
        info!("Prev: {}", prev_seq);
        loop {
            let pos_segment = self.pos_segment.lock().unwrap();
            match pos_segment.read_message(prev_seq, &mut body) {
                ReadResult::Nothing => {
                    return None;
                }
                ReadResult::Retry => {
                    continue;
                }
                ReadResult::Success(seq, publish_micros) => {
                    self.read_seq = seq + 1;
                    return Some(Inbound {
                        seq: seq,
                        body: body,
                        publish_micros: publish_micros,
                    });
                }
            }
        }
    }

    pub fn new(shared_fd: OwnedFd, data: &[u8], latch: bool) -> Result<ReadSegment, Error> {
        info!("Data Len: {}", data.len());
        assert!(data.len() == 8);
        let wire_segment_uid = u64::from_ne_bytes(data[0..8].try_into().unwrap());
        let segment_rc = Arc::new(Mutex::new(UniqueReadSegment::new(shared_fd)?));

        let mut max_seq = 0;
        let segment_uid;
        let n_messages;
        {
            let segment = segment_rc.lock().unwrap();
            // read header
            segment_uid = segment.mem_fd.read_u64_at(ABS_POS_SEGMENT_UID);
            if wire_segment_uid != segment_uid {
                return Err(Error::new(format!(
                    "Topic id sent through socket ({}) \
                    does not match the segment_uid in the shared mem: {}",
                    wire_segment_uid, segment_uid
                )));
            }
            n_messages = segment.mem_fd.read_u32_at(ABS_POS_N_MESSAGES);

            for i in 0..n_messages {
                let meta_pos = PRIMARY_HEADER_SIZE + MESSAGE_HEADER_SIZE * (i as usize);
                let read_seq = segment.mem_fd.read_u64_at(meta_pos + MSG_POS_SEQ);
                max_seq = u64::max(max_seq, read_seq);
            }
        }

        // must always start >= 2 because 0, 1 are sentinals
        let first_seq;
        if !latch {
            // no latching, start after last seq
            first_seq = u64::max(max_seq + 1, 2);
        } else {
            // latching, start at last
            first_seq = u64::max(max_seq, 2);
        }

        return Ok(ReadSegment {
            segment_uid: segment_uid,
            pos_segment: segment_rc,
            read_seq: first_seq,
        });
    }
}
