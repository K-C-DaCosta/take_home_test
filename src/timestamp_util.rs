use serde::{Deserialize, Serialize};
#[derive(Copy, Clone, Serialize, Deserialize)]
pub struct TimeStampInfo {
    //according to chrono docs this is the time in seconds since 1970's or something like that
    pub timestamp: i64,
    // url_id is simply just a unique number that acts as an alias for the actual url
    // this is done for compression reasons
    pub url_id: u64,
}

impl std::fmt::Display for TimeStampInfo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "[ts:{},id:{}]", self.timestamp, self.url_id)
    }
}
#[derive(Serialize, Deserialize, Default)]
pub struct TimestampChunkHeader {
    pub min_time: i64,
    pub max_time: i64,
}
//every binary chunk that gets written to disk will be in this
//format
#[derive(Serialize, Deserialize)]
pub struct TimestampChunk {
    pub header: TimestampChunkHeader,
    pub timestamp_list: Vec<TimeStampInfo>,
}

impl TimestampChunk {
    pub fn new() -> Self {
        Self {
            header: TimestampChunkHeader::default(),
            timestamp_list: Vec::new(),
        }
    }
}