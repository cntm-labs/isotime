use std::sync::atomic::AtomicU64;
use memmap2::MmapMut;
use std::fs::OpenOptions;
use std::path::Path;

#[repr(C, align(128))]
pub struct BusHeader {
    pub magic_number: u64,
    pub version: u32,
    pub capacity: u32,
    pub head: AtomicU64,
    pub tail: AtomicU64,
    pub writer_pid: u32,
    pub last_heartbeat: AtomicU64,
    pub overflow_count: AtomicU64,
    pub _reserved: [u8; 76], // Explicit padding to reach 128 bytes
}

#[repr(C, align(128))]
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct DeltaEvent {
    pub event_id: u64,
    pub event_type: u8,
    pub _reserved: [u8; 7], // Alignment padding
    pub timestamp: u64,
    pub payload: [u8; 96],
    pub checksum: u64,
}

impl Default for DeltaEvent {
    fn default() -> Self {
        Self {
            event_id: 0,
            event_type: 0,
            _reserved: [0; 7],
            timestamp: 0,
            payload: [0; 96],
            checksum: 0,
        }
    }
}

pub struct BusManager {
    mmap: MmapMut,
}

impl BusManager {
    pub const MAGIC: u64 = 0x49534F54494D4542; // "ISOTIMEB"
    pub const SLOT_SIZE: usize = 128;
    
    pub fn new<P: AsRef<Path>>(path: P, capacity: u32) -> std::io::Result<Self> {
        let size = std::mem::size_of::<BusHeader>() + (capacity as usize * Self::SLOT_SIZE);
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(path)?;
        file.set_len(size as u64)?;
        let mut mmap = unsafe { MmapMut::map_mut(&file)? };
        
        // Initialize header if new
        let header = unsafe { &mut *(mmap.as_mut_ptr() as *mut BusHeader) };
        if header.magic_number != Self::MAGIC {
            header.magic_number = Self::MAGIC;
            header.version = 1;
            header.capacity = capacity;
            header.head.store(0, std::sync::atomic::Ordering::SeqCst);
            header.tail.store(0, std::sync::atomic::Ordering::SeqCst);
            header.overflow_count.store(0, std::sync::atomic::Ordering::SeqCst);
            header.last_heartbeat.store(0, std::sync::atomic::Ordering::SeqCst);
        }
        
        Ok(Self { mmap })
    }

    #[inline]
    fn header(&self) -> &BusHeader {
        unsafe { &*(self.mmap.as_ptr() as *const BusHeader) }
    }

    #[inline]
    fn slots_mut(&mut self) -> &mut [DeltaEvent] {
        let ptr = unsafe { self.mmap.as_mut_ptr().add(std::mem::size_of::<BusHeader>()) };
        // Ensure ptr is aligned to 128
        assert_eq!(ptr as usize % 128, 0, "Slots pointer must be 128-byte aligned");
        unsafe { std::slice::from_raw_parts_mut(ptr as *mut DeltaEvent, self.header().capacity as usize) }
    }

    pub fn push(&mut self, mut event: DeltaEvent) -> bool {
        let head = self.header().head.load(std::sync::atomic::Ordering::Acquire);
        let tail = self.header().tail.load(std::sync::atomic::Ordering::Acquire);
        let capacity = self.header().capacity as u64;

        if tail >= head + capacity {
            self.header().overflow_count.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            return false;
        }

        let index = (tail % capacity) as usize;
        
        // Calculate checksum over everything except the checksum field itself
        event.checksum = 0;
        let ptr = &event as *const DeltaEvent as *const u8;
        let data = unsafe { std::slice::from_raw_parts(ptr, 120) };
        event.checksum = crc64::crc64(0, data);

        self.slots_mut()[index] = event;
        self.header().tail.fetch_add(1, std::sync::atomic::Ordering::Release);
        true
    }

    pub fn pop_batch(&mut self, limit: usize) -> Vec<DeltaEvent> {
        let head = self.header().head.load(std::sync::atomic::Ordering::Acquire);
        let tail = self.header().tail.load(std::sync::atomic::Ordering::Acquire);
        let capacity = self.header().capacity as u64;
        
        let available = (tail - head) as usize;
        let count = std::cmp::min(available, limit);
        let mut batch = Vec::with_capacity(count);

        for i in 0..count {
            let index = ((head + i as u64) % capacity) as usize;
            let event = self.slots_mut()[index];
            
            // Verify checksum
            let mut event_copy = event;
            event_copy.checksum = 0;
            let ptr = &event_copy as *const DeltaEvent as *const u8;
            let data = unsafe { std::slice::from_raw_parts(ptr, 120) };
            let expected_sum = crc64::crc64(0, data);
            
            if event.checksum == expected_sum {
                batch.push(event);
            }
        }

        self.header().head.fetch_add(batch.len() as u64, std::sync::atomic::Ordering::Release);
        batch
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    #[test]
    fn test_bus_push_pop_cycle() {
        let bus_path = "test_bus.bin";
        if Path::new(bus_path).exists() {
            fs::remove_file(bus_path).unwrap();
        }

        let mut bus = BusManager::new(bus_path, 1024).unwrap();
        let event = DeltaEvent {
            event_id: 1,
            event_type: 1,
            _reserved: [0; 7],
            timestamp: 100,
            payload: [0xAA; 96],
            checksum: 0,
        };

        assert!(bus.push(event));
        let batch = bus.pop_batch(10);
        assert_eq!(batch.len(), 1);
        assert_eq!(batch[0].event_id, 1);
        assert_eq!(batch[0].payload[0], 0xAA);

        fs::remove_file(bus_path).unwrap();
    }
}
