use crate::storage::compressor::CompressionPolicy;
use crate::storage::StorageEngine;
use std::ffi::{c_char, c_void, CStr};
use std::ptr;
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

struct FfiEngine {
    engine: StorageEngine,
    runtime: Runtime,
}

/// Opens an isotime storage engine.
///
/// # Safety
/// - `wal_path` and `cas_path` must be valid, null-terminated C strings.
/// - `encryption_key` must be a null pointer OR a pointer to at least 32 bytes of memory.
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
        Ok(r) => r,
        Err(_) => return ptr::null_mut(),
    };

    let engine_res = rt.block_on(StorageEngine::new(
        wal_str.as_ref(),
        key,
        policy.into(),
        cas_str.as_ref(),
    ));

    match engine_res {
        Ok(engine) => Box::into_raw(Box::new(FfiEngine { engine, runtime: rt })) as *mut c_void,
        Err(_) => ptr::null_mut(),
    }
}

/// Closes the storage engine and frees associated memory.
///
/// # Safety
/// - `engine_ptr` must be a pointer returned by `isotime_open`.
#[no_mangle]
pub unsafe extern "C" fn isotime_close(engine_ptr: *mut c_void) {
    if !engine_ptr.is_null() {
        let _ = Box::from_raw(engine_ptr as *mut FfiEngine);
    }
}

/// Retrieves a value from the storage engine.
///
/// # Safety
/// - `engine_ptr` must be a valid pointer to an internal FFI engine handle.
/// - `key_data` must be a valid pointer to at least `key_len` bytes of memory.
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
///
/// # Safety
/// - `buffer` must be a struct returned by `isotime_get` and not previously freed.
#[no_mangle]
pub unsafe extern "C" fn isotime_free_buffer(buffer: IsotimeBuffer) {
    if !buffer.data.is_null() && buffer.len > 0 {
        let _ = Vec::from_raw_parts(buffer.data, buffer.len, buffer.len);
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
            .block_on(ffi.engine.put(b"ffi_key".to_vec(), b"ffi_value".to_vec()))
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
}
