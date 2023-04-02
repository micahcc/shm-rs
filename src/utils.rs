use crc::{Crc, CRC_32_CKSUM};

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
