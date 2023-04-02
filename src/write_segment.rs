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
use crate::shared_write_segment::SharedWriteSegment;
use crate::utils::{compute_crc32, now_micros};
use crate::wire_message::WireMessage;
use crate::write_interface::WriteInterface;
use log::{error, info, warn};
use rand::Rng;
use std::sync::{Arc, Mutex};

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

    pos_segment: Arc<Mutex<SharedWriteSegment>>,

    next_seq: u64,
}

impl WriteSegment {
    pub fn new(name: &str, n_bytes: usize, n_messages: u32) -> Result<WriteSegment, Error> {
        let mut rng = rand::thread_rng();
        let segment_uid: u64 = rng.gen();

        let segment_rc = Arc::new(Mutex::new(SharedWriteSegment::new(
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

    pub fn get_write_buffer(&mut self) -> Result<WriteInterface, Error> {
        let position: usize;
        let write_seq: u64;
        {
            let mut pos_segment = self.pos_segment.lock().unwrap();
            position = pos_segment.start_write()?;
            write_seq = self.next_seq;
            self.next_seq += 1;
        }

        return Ok(WriteInterface {
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
