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
use crate::errors::SocketError;
use crate::mem_fd::MemFd;
use crate::utils::{compute_crc32, now_micros};
use crate::wire_message::WireMessage;
use log::{error, info, warn};
use rand::Rng;
use std::sync::{Arc, Mutex};

// We may need to share the same write memory between many users, so keep a lock on the
// actual memory and store it in UniqueWriteSegment
struct UniqueWriteSegment {
    mem_fd: MemFd,

    // where to write next in memory
    // every time we call allocate we advance this forward and invalidate
    // any headers for messages that we need to overwrite
    write_position: usize,

    n_messages: u32,
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

impl UniqueWriteSegment {
    fn new(name: &str, n_bytes: usize, n_messages: u32) -> Result<UniqueWriteSegment, SocketError> {
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

        return Ok(UniqueWriteSegment {
            mem_fd: mem_fd,
            write_position: write_position,
            n_messages: n_messages,
        });
    }

    fn start_write(&mut self) -> Result<usize, SocketError> {
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
            return Err(SocketError::new(format!(
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

    fn complete_write(&mut self, meta_position: usize, seq: u64) {
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

    fn allocate_block(
        &mut self,
        alloc_meta_pos: usize,
        is_body: bool,
        alloc_len: usize,
    ) -> Result<*mut u8, SocketError> {
        if alloc_len > self.mem_fd.len() {
            return Err(SocketError::new(format!(
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
                    return Err(SocketError::new(format!(
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

pub struct WriteBuffer {
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
    meta_position: usize,

    // reserved sequence that we will write when we're done
    seq: u64,

    // cached so we don't have to lock to get it
    segment_uid: u64,

    // memory we'll write to, we want to keep this entire block alive while write
    // buffer is alive (because we have an unsafe pointer)
    pos_segment: Arc<Mutex<UniqueWriteSegment>>,
}

impl std::fmt::Debug for WriteBuffer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WriteBuffer")
            .field("meta_position", &self.meta_position)
            .field("seq", &self.seq)
            .finish()
    }
}

// Write buffer is passed to the user application code giving them the ability
// to write to a segment.
impl WriteBuffer {
    pub fn get_seq(&self) -> u64 {
        return self.seq;
    }

    pub fn get_segment_uid(&self) -> u64 {
        return self.segment_uid;
    }

    pub fn alloc_slice(&mut self, len: usize) -> Result<&mut [u8], SocketError> {
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

impl Drop for WriteBuffer {
    /// Ensures that any memory associated with the WriteBuffer was actually
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

// Because I don't want to do defragmentation I'm going to fix the number
// of messages and make the header constant, this also means that there isn't
// much point in moving the messages around in memory (we're going to have
// the same number of messages and memory regardless). You might get a small
// benefit of having 1 huge message and 3 tiny messages if you move the boundaries
// around, but it complicates things and could results in unexpectedly hitting
// message limits.
// Long story short, message starts are fixed.

// layout
//  0  -- uint64 segment_uid, matching what is sent in topology and socket
//  8  -- uint32 max message count,
// 12  -- uint8[4] padding

// For each message in max message count, starting at 16:
// ------ Repeat every 40 bytes ----
// u64  0 -- seq
// u64  8 -- send_timestamp
// u32  16 -- offset
// u32  20 -- size
// u32  24 -- crc
//      28 -- end

pub struct WriteSegment {
    pub segment_uid: u64,

    pos_segment: Arc<Mutex<UniqueWriteSegment>>,

    next_seq: u64,
}

impl WriteSegment {
    pub fn new(name: &str, n_bytes: usize, n_messages: u32) -> Result<WriteSegment, SocketError> {
        let mut rng = rand::thread_rng();
        let segment_uid: u64 = rng.gen();

        let segment_rc = Arc::new(Mutex::new(UniqueWriteSegment::new(
            name, n_bytes, n_messages,
        )?));
        {
            let mut segment = segment_rc.lock().unwrap();

            // write out header
            segment
                .mem_fd
                .write_u64_at(ABS_POS_SEGMENT_UID, segment_uid);
            segment.mem_fd.write_u32_at(ABS_POS_N_MESSAGES, n_messages);
            segment.mem_fd.write_u8_at(12, 0); // pad
            segment.mem_fd.write_u8_at(13, 0); // pad
            segment.mem_fd.write_u8_at(14, 0); // pad
            segment.mem_fd.write_u8_at(15, 0); // pad
            assert!(PRIMARY_HEADER_SIZE == 16);
        }

        return Ok(WriteSegment {
            segment_uid: segment_uid,
            pos_segment: segment_rc,
            next_seq: 2,
        });
    }

    pub fn get_write_buffer(&mut self) -> Result<WriteBuffer, SocketError> {
        let position: usize;
        let write_seq: u64;
        {
            let mut pos_segment = self.pos_segment.lock().unwrap();
            position = pos_segment.start_write()?;
            write_seq = self.next_seq;
            self.next_seq += 1;
        }

        return Ok(WriteBuffer {
            pos_segment: self.pos_segment.clone(),
            segment_uid: self.segment_uid,
            meta_position: position,
            seq: write_seq,
        });
    }

    pub fn to_wire(&self) -> WireMessage {
        let pos_segment = self.pos_segment.lock().unwrap();
        let data = self.segment_uid.to_ne_bytes().to_vec();
        info!("Sending {} bytes", data.len());
        let fds = vec![pos_segment.mem_fd.to_owned_fd()];
        return WireMessage {
            data: data,
            fds: fds,
        };
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn init() {
        let _ = env_logger::builder().is_test(true).try_init();
    }

    #[test]
    fn segment_basics() {
        init();

        let memory_size: u32 = 100;
        let n_messages = 2;
        let body_start = PRIMARY_HEADER_SIZE + MESSAGE_HEADER_SIZE * (n_messages as usize);
        let body_size = memory_size as usize - body_start;
        let mut segment = WriteSegment::new(
            "hello",
            memory_size as usize, /* bytes */
            n_messages,           /* n_messages */
            SegmentPurpose::PubSub,
        )
        .expect("Should succeed");

        {
            let mut buffer1 = segment.get_write_buffer().expect("Should be good");
            let mut _buffer2 = segment.get_write_buffer().expect("Should be good");
            segment
                .get_write_buffer()
                .expect_err("Should fail due to using all the message slots");

            // now write a message
            let mem1 = buffer1
                .alloc_body_slice(body_size as usize)
                .expect("Should be ok");
            for i in 0..body_size {
                mem1[i as usize] = i as u8;
            }

            buffer1.complete_write();

            // check the header shows buffer1 filled;
            let pos_segment = segment.pos_segment.lock().unwrap();
            let slice = pos_segment.mem_fd.slice();

            // segment_uid should be first
            assert!(slice[0..8] == segment.segment_uid.to_ne_bytes());

            // max number of messages should be = 2
            assert!(slice[8..12] == n_messages.to_ne_bytes());

            // (1) segment header
            // seq should be filled with 2
            assert!(slice[16..24] == (2 as u64).to_ne_bytes());

            // head offset + size should be zero
            assert!(slice[32..36] == (0 as u32).to_ne_bytes());
            assert!(slice[36..40] == (0 as u32).to_ne_bytes());

            // body offset should be right after the main header
            assert!(slice[44..48] == (body_start as u32).to_ne_bytes());

            // body size should be ok
            assert!(slice[48..52] == (body_size as u32).to_ne_bytes());

            // (2) segment header
            // seq should be filled with 0 (NO WRITING ALLOWED)
            assert!(slice[56..64] == (0 as u64).to_ne_bytes());

            // head offset + size should be zero
            assert!(slice[72..76] == (0 as u32).to_ne_bytes());
            assert!(slice[76..80] == (0 as u32).to_ne_bytes());

            // body offset + size should be zero
            assert!(slice[84..88] == (0 as u32).to_ne_bytes());
            assert!(slice[88..92] == (0 as u32).to_ne_bytes());
        }

        {
            let pos_segment = segment.pos_segment.lock().unwrap();
            let slice = pos_segment.mem_fd.slice();
            // now that we failed to write the second message, the seq should be returned to 1
            // (2) segment header
            // seq should be back to 1, since we didn't ever finish it
            assert!(slice[56..64] == (1 as u64).to_ne_bytes());

            // head offset + size should be zero
            assert!(slice[72..76] == (0 as u32).to_ne_bytes());
            assert!(slice[76..80] == (0 as u32).to_ne_bytes());

            // body offset + size should be zero
            assert!(slice[84..88] == (0 as u32).to_ne_bytes());
            assert!(slice[88..92] == (0 as u32).to_ne_bytes());
        }

        // next buffer should overwrite message 2
        {
            let mut buffer = segment.get_write_buffer().expect("Should be good");
            {
                let pos_segment = segment.pos_segment.lock().unwrap();
                let slice = pos_segment.mem_fd.slice();

                // since we're using it, message 2 should have seq of 0
                assert!(slice[56..64] == (0 as u64).to_ne_bytes());

                // head offset + size should be zero
                assert!(slice[72..76] == (0 as u32).to_ne_bytes());
                assert!(slice[76..80] == (0 as u32).to_ne_bytes());

                // body offset + size should be zero
                assert!(slice[84..88] == (0 as u32).to_ne_bytes());
                assert!(slice[88..92] == (0 as u32).to_ne_bytes());
            }

            // writing to body should fill the body
            let head1 = buffer.alloc_head_slice(1).expect("Should be ok");
            head1[0] = 19;
            let body1 = buffer.alloc_body_slice(1).expect("Should be ok");
            body1[0] = 17;
            buffer.complete_write();

            // head offset should be first body position
            // head size should be 1
            {
                let pos_segment = segment.pos_segment.lock().unwrap();
                let slice = pos_segment.mem_fd.slice();

                assert!(slice[72..76] == (body_start as u32).to_ne_bytes()); // head offset
                assert!(slice[76..80] == (1 as u32).to_ne_bytes()); // head size
                assert!(slice[body_start] == 19); // head value

                // body offset should be body_start + 1
                // body size should be 1
                assert!(slice[84..88] == ((body_start + 1) as u32).to_ne_bytes()); // body offset
                assert!(slice[88..92] == (1 as u32).to_ne_bytes()); // body size
                assert!(slice[body_start + 1] == 17);
            }
        }

        // next buffer should overwrite message 1
        {
            let mut buffer = segment.get_write_buffer().expect("Should be good");
            {
                let pos_segment = segment.pos_segment.lock().unwrap();
                let slice = pos_segment.mem_fd.slice();

                // since we're using it, message 1 should have seq of 0
                assert!(slice[16..24] == (0 as u64).to_ne_bytes());

                // head offset + size should be zero
                assert!(slice[32..36] == (0 as u32).to_ne_bytes());
                assert!(slice[36..40] == (0 as u32).to_ne_bytes());

                // body offset + size should be zero
                assert!(slice[44..48] == (0 as u32).to_ne_bytes());
                assert!(slice[48..52] == (0 as u32).to_ne_bytes());
            }

            // writing to body should fill the body, 3 bytes should roll around
            // and start again at the beginning
            let body1 = buffer.alloc_body_slice(3).expect("Should be ok");
            body1[0] = 27;
            body1[1] = 28;
            body1[2] = 29;

            // 1 byte should finish off
            let head1 = buffer.alloc_head_slice(1).expect("Should be ok");
            head1[0] = 23;
            buffer.complete_write();

            // head offset should be first body position
            // head size should be 1
            {
                let pos_segment = segment.pos_segment.lock().unwrap();
                let slice = pos_segment.mem_fd.slice();

                // body offset should be body_start
                // body size should be 3
                assert!(slice[44..48] == (body_start as u32).to_ne_bytes()); // head offset
                assert!(slice[48..52] == (3 as u32).to_ne_bytes()); // head size
                assert!(slice[body_start + 0] == 27);
                assert!(slice[body_start + 1] == 28);
                assert!(slice[body_start + 2] == 29);

                // head offset should be body_start + 3
                // head size should be 1
                assert!(slice[32..36] == ((body_start + 3) as u32).to_ne_bytes()); // body offset
                assert!(slice[36..40] == (1 as u32).to_ne_bytes()); // body size
                assert!(slice[body_start + 3] == 23);
            }
        }
    }

    #[test]
    fn segment_hard() {
        init();

        // for a range of parameters test against a destructive version of the
        // algorithm that writes one byte for the entirety of each message

        for body_size in [100, 1024, 100000] {
            for n_messages in [1, 10, 100] {
                let body_start = PRIMARY_HEADER_SIZE + MESSAGE_HEADER_SIZE * (n_messages as usize);
                let memory_size = body_size + body_start;
                let mut segment = WriteSegment::new(
                    "hello",
                    memory_size as usize, /* bytes */
                    n_messages,           /* n_messages */
                    SegmentPurpose::PubSub,
                )
                .expect("Should succeed");

                for _ in 1..253 {
                    // now write a message
                    let mut buffer1 = segment.get_write_buffer().expect("Should be good");
                    let mem1 = buffer1
                        .alloc_body_slice(body_size as usize)
                        .expect("Should be ok");
                    for i in 0..body_size {
                        mem1[i as usize] = i as u8;
                    }

                    buffer1.complete_write();

                    // check the header shows buffer1 filled;
                    let pos_segment = segment.pos_segment.lock().unwrap();
                    let _slice = pos_segment.mem_fd.slice();
                }
            }
        }
    }
}
