use std::sync::atomic::AtomicU64;

#[repr(C, align(64))]
pub struct BusHeader {
    pub magic_number: u64,
    pub version: u32,
    pub capacity: u32,
    pub head: AtomicU64,
    pub tail: AtomicU64,
    pub writer_pid: u32,
    pub last_heartbeat: AtomicU64,
    pub overflow_count: AtomicU64,
}

#[repr(C, align(128))]
#[derive(Clone, Copy)]
pub struct DeltaEvent {
    pub event_id: u64,
    pub event_type: u8, // 0: NodeAdd, 1: NodeUpdate, 2: NodeDelete, 3: EdgeAdd, etc.
    pub timestamp: u64,
    pub payload: [u8; 104],
    pub checksum: u64,
}

impl Default for DeltaEvent {
    fn default() -> Self {
        Self {
            event_id: 0,
            event_type: 0,
            timestamp: 0,
            payload: [0; 104],
            checksum: 0,
        }
    }
}
