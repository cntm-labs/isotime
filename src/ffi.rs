use crate::storage::compressor::CompressionPolicy;
use crate::storage::query::QueryBuilder;
use crate::storage::StorageEngine;
use std::ffi::{c_char, c_void, CStr};
use std::ptr;
use std::sync::Arc;
use tokio::runtime::Runtime;

#[repr(C)]
pub enum FfiCompressionPolicy {
    Fastest = 0,
    Balanced = 1,
    ExtremeSpace = 2,
}

impl From<FfiCompressionPolicy> for CompressionPolicy {
    fn from(p: FfiCompressionPolicy) -> Self {
        match p {
            FfiCompressionPolicy::Fastest => CompressionPolicy::Fastest,
            FfiCompressionPolicy::Balanced => CompressionPolicy::Balanced,
            FfiCompressionPolicy::ExtremeSpace => CompressionPolicy::ExtremeSpace,
        }
    }
}

#[repr(C)]
pub struct IsotimeBuffer {
    pub data: *mut u8,
    pub len: usize,
}

#[repr(C)]
pub struct IsotimeEntry {
    pub key_data: *const u8,
    pub key_len: usize,
    pub val_data: *const u8,
    pub val_len: usize,
}

struct FfiEngine {
    engine: Arc<StorageEngine>,
    runtime: Arc<Runtime>,
}

struct FfiQuery {
    builder: Option<QueryBuilder>,
    runtime: Arc<Runtime>,
}

struct FfiResultSet {
    results: Vec<(Vec<u8>, Vec<u8>)>,
    cursor: usize,
}

/// Opens an isotime storage engine.
#[no_mangle]
pub unsafe extern "C" fn isotime_open(
    wal_path: *const c_char,
    cas_path: *const c_char,
    encryption_key: *const u8,
    policy: FfiCompressionPolicy,
) -> *mut c_void {
    if wal_path.is_null() || cas_path.is_null() {
        return ptr::null_mut();
    }

    let wal_str = CStr::from_ptr(wal_path).to_string_lossy();
    let cas_str = CStr::from_ptr(cas_path).to_string_lossy();

    let key = if encryption_key.is_null() {
        None
    } else {
        let mut k = [0u8; 32];
        ptr::copy_nonoverlapping(encryption_key, k.as_mut_ptr(), 32);
        Some(k)
    };

    let rt = match Runtime::new() {
        Ok(r) => Arc::new(r),
        Err(_) => return ptr::null_mut(),
    };

    let engine_res = rt.block_on(StorageEngine::new(
        wal_str.as_ref(),
        key,
        policy.into(),
        cas_str.as_ref(),
    ));

    match engine_res {
        Ok(engine) => Box::into_raw(Box::new(FfiEngine {
            engine: Arc::new(engine),
            runtime: rt,
        })) as *mut c_void,
        Err(_) => ptr::null_mut(),
    }
}

/// Closes the storage engine and frees associated memory.
#[no_mangle]
pub unsafe extern "C" fn isotime_close(engine_ptr: *mut c_void) {
    if !engine_ptr.is_null() {
        let _ = Box::from_raw(engine_ptr as *mut FfiEngine);
    }
}

/// Puts a value into the storage engine.
#[no_mangle]
pub unsafe extern "C" fn isotime_put(
    engine_ptr: *mut c_void,
    key_data: *const u8,
    key_len: usize,
    val_data: *const u8,
    val_len: usize,
) {
    if engine_ptr.is_null() || key_data.is_null() || val_data.is_null() {
        return;
    }
    let ffi = &*(engine_ptr as *mut FfiEngine);
    let key = std::slice::from_raw_parts(key_data, key_len).to_vec();
    let val = std::slice::from_raw_parts(val_data, val_len).to_vec();

    let _ = ffi
        .runtime
        .block_on(ffi.engine.put(key, val, vec![], vec![]));
}

/// Retrieves a value from the storage engine.
#[no_mangle]
pub unsafe extern "C" fn isotime_get(
    engine_ptr: *mut c_void,
    key_data: *const u8,
    key_len: usize,
) -> IsotimeBuffer {
    if engine_ptr.is_null() || key_data.is_null() || key_len == 0 {
        return IsotimeBuffer {
            data: ptr::null_mut(),
            len: 0,
        };
    }

    let ffi = &*(engine_ptr as *mut FfiEngine);
    let key = std::slice::from_raw_parts(key_data, key_len);

    match ffi.runtime.block_on(ffi.engine.get(key)) {
        Ok(Some(mut val)) => {
            val.shrink_to_fit();
            let len = val.len();
            let data = val.as_mut_ptr();
            std::mem::forget(val);
            IsotimeBuffer { data, len }
        }
        _ => IsotimeBuffer {
            data: ptr::null_mut(),
            len: 0,
        },
    }
}

/// Frees a buffer returned by `isotime_get`.
#[no_mangle]
pub unsafe extern "C" fn isotime_free_buffer(buffer: IsotimeBuffer) {
    if !buffer.data.is_null() && buffer.len > 0 {
        let _ = Vec::from_raw_parts(buffer.data, buffer.len, buffer.len);
    }
}

// --- Query API ---

#[no_mangle]
pub unsafe extern "C" fn isotime_query_new(engine_ptr: *mut c_void) -> *mut c_void {
    if engine_ptr.is_null() {
        return ptr::null_mut();
    }
    let ffi = &*(engine_ptr as *mut FfiEngine);
    let builder = ffi.engine.query();
    Box::into_raw(Box::new(FfiQuery {
        builder: Some(builder),
        runtime: Arc::clone(&ffi.runtime),
    })) as *mut c_void
}

#[no_mangle]
pub unsafe extern "C" fn isotime_query_tag(query_ptr: *mut c_void, tag: *const c_char) {
    if query_ptr.is_null() || tag.is_null() {
        return;
    }
    let ffi = &mut *(query_ptr as *mut FfiQuery);
    let tag_str = CStr::from_ptr(tag).to_string_lossy();
    if let Some(builder) = ffi.builder.take() {
        ffi.builder = Some(builder.tag(tag_str.into_owned()));
    }
}

#[no_mangle]
pub unsafe extern "C" fn isotime_query_range(query_ptr: *mut c_void, min: f64, max: f64) {
    if query_ptr.is_null() {
        return;
    }
    let ffi = &mut *(query_ptr as *mut FfiQuery);
    if let Some(builder) = ffi.builder.take() {
        ffi.builder = Some(builder.range(min, max));
    }
}

#[no_mangle]
pub unsafe extern "C" fn isotime_query_after(
    query_ptr: *mut c_void,
    clock_ptr: *const u64,
    clock_len: usize,
) {
    if query_ptr.is_null() || clock_ptr.is_null() || clock_len % 2 != 0 {
        return;
    }
    let ffi = &mut *(query_ptr as *mut FfiQuery);
    let raw_clock = std::slice::from_raw_parts(clock_ptr, clock_len);
    let mut clock = Vec::with_capacity(clock_len / 2);
    for i in (0..clock_len).step_by(2) {
        clock.push((raw_clock[i] as u32, raw_clock[i + 1]));
    }
    if let Some(builder) = ffi.builder.take() {
        ffi.builder = Some(builder.after(clock));
    }
}

#[no_mangle]
pub unsafe extern "C" fn isotime_query_execute(query_ptr: *mut c_void) -> *mut c_void {
    if query_ptr.is_null() {
        return ptr::null_mut();
    }
    let mut ffi = Box::from_raw(query_ptr as *mut FfiQuery);
    if let Some(builder) = ffi.builder.take() {
        let results = ffi.runtime.block_on(builder.execute());
        Box::into_raw(Box::new(FfiResultSet { results, cursor: 0 })) as *mut c_void
    } else {
        ptr::null_mut()
    }
}

#[no_mangle]
pub unsafe extern "C" fn isotime_result_next(result_ptr: *mut c_void) -> IsotimeEntry {
    if result_ptr.is_null() {
        return IsotimeEntry {
            key_data: ptr::null(),
            key_len: 0,
            val_data: ptr::null(),
            val_len: 0,
        };
    }
    let ffi = &mut *(result_ptr as *mut FfiResultSet);
    if ffi.cursor < ffi.results.len() {
        let (ref key, ref val) = ffi.results[ffi.cursor];
        ffi.cursor += 1;
        IsotimeEntry {
            key_data: key.as_ptr(),
            key_len: key.len(),
            val_data: val.as_ptr(),
            val_len: val.len(),
        }
    } else {
        IsotimeEntry {
            key_data: ptr::null(),
            key_len: 0,
            val_data: ptr::null(),
            val_len: 0,
        }
    }
}

#[no_mangle]
pub unsafe extern "C" fn isotime_result_free(result_ptr: *mut c_void) {
    if !result_ptr.is_null() {
        let _ = Box::from_raw(result_ptr as *mut FfiResultSet);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::ffi::CString;
    use tempfile::tempdir;

    #[test]
    fn test_ffi_lifecycle_and_get() {
        let cas_dir = tempdir().unwrap();
        let wal_path = CString::new("test_ffi.wal").unwrap();
        let cas_path = CString::new(cas_dir.path().to_str().unwrap()).unwrap();

        let engine_ptr = unsafe {
            isotime_open(
                wal_path.as_ptr(),
                cas_path.as_ptr(),
                ptr::null(),
                FfiCompressionPolicy::Balanced,
            )
        };
        assert!(!engine_ptr.is_null());

        let ffi = unsafe { &*(engine_ptr as *mut FfiEngine) };
        ffi.runtime
            .block_on(
                ffi.engine
                    .put(b"ffi_key".to_vec(), b"ffi_value".to_vec(), vec![], vec![]),
            )
            .unwrap();

        let key = b"ffi_key";
        let buffer = unsafe { isotime_get(engine_ptr, key.as_ptr(), key.len()) };
        assert!(!buffer.data.is_null());
        assert_eq!(buffer.len, 9);

        let retrieved = unsafe { std::slice::from_raw_parts(buffer.data, buffer.len) };
        assert_eq!(retrieved, b"ffi_value");

        unsafe { isotime_free_buffer(buffer) };

        let bad_key = b"not_exist";
        let bad_buffer = unsafe { isotime_get(engine_ptr, bad_key.as_ptr(), bad_key.len()) };
        assert!(bad_buffer.data.is_null());

        unsafe { isotime_close(engine_ptr) };
        let _ = std::fs::remove_file("test_ffi.wal");
    }

    #[test]
    fn test_ffi_query_integration() {
        let cas_dir = tempdir().unwrap();
        let wal_path = CString::new("test_ffi_query.wal").unwrap();
        let cas_path = CString::new(cas_dir.path().to_str().unwrap()).unwrap();

        let engine_ptr = unsafe {
            isotime_open(
                wal_path.as_ptr(),
                cas_path.as_ptr(),
                ptr::null(),
                FfiCompressionPolicy::Balanced,
            )
        };

        let ffi = unsafe { &*(engine_ptr as *mut FfiEngine) };
        ffi.runtime
            .block_on(ffi.engine.put(
                b"q1".to_vec(),
                10.5f64.to_le_bytes().to_vec(),
                vec!["iot".into()],
                vec![],
            ))
            .unwrap();
        ffi.runtime
            .block_on(ffi.engine.put(
                b"q2".to_vec(),
                50.0f64.to_le_bytes().to_vec(),
                vec!["iot".into()],
                vec![],
            ))
            .unwrap();

        unsafe {
            let query = isotime_query_new(engine_ptr);
            let tag = CString::new("iot").unwrap();
            isotime_query_tag(query, tag.as_ptr());
            isotime_query_range(query, 0.0, 20.0);

            let results = isotime_query_execute(query);
            let entry = isotime_result_next(results);

            assert!(!entry.key_data.is_null());
            let key = std::slice::from_raw_parts(entry.key_data, entry.key_len);
            assert_eq!(key, b"q1");

            let done = isotime_result_next(results);
            assert!(done.key_data.is_null());

            isotime_result_free(results);
            isotime_close(engine_ptr);
        }
        let _ = std::fs::remove_file("test_ffi_query.wal");
    }
}
