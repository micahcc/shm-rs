// Up to max_messages is stored concurrently. Any reader may be at a different
// place, but data is invalidated when a new write is performed and the max number
// of messages are filled.

// Because I don't want to do defragmentation I'm going to fix the number
// of messages and make the header constant.

// layout
//  0  -- uint64 topic_id, matching what is sent in topology and socket
//  8  -- uint32 max message count,
// 12  -- uint8[4] padding
// 16  -- end

// For each message in max message count, starting at 16:
// ------ Repeat every 28 bytes ----
// u64  0 -- seq
// u64  8 -- send_timestamp
// u32  16 -- offset
// u32  20 -- size
// u32  24 -- crc
//      28 -- end

// After 28 x N_MESSAGES the rest of the member is the slab of current messages.

pub const PRIMARY_HEADER_SIZE: usize = 16;
pub const MESSAGE_HEADER_SIZE: usize = 28;

// Absolute positions of primary header
pub const ABS_POS_SEGMENT_UID: usize = 0;
pub const ABS_POS_N_MESSAGES: usize = 8;

// Positions within a message header section
pub const MSG_POS_SEQ: usize = 0;
pub const MSG_POS_TIMESTAMP: usize = 8;
pub const MSG_POS_OFFSET: usize = 16;
pub const MSG_POS_SIZE: usize = 20;
pub const MSG_POS_CRC: usize = 24;
