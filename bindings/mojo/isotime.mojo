from utils.index import Index
from memory import Pointer
from ffi import DLHandle, ExternalCompilationUnit
from buffer import Buffer

@value
struct IsotimeBuffer(CollectionElement):
    var data: Pointer[UInt8]
    var len: Int

    fn __init__(inout self, data: Pointer[UInt8], len: Int):
        self.data = data
        self.len = len

struct Isotime:
    var handle: DLHandle
    var engine: Pointer[None]

    fn __init__(inout self, lib_path: String):
        self.handle = DLHandle(lib_path)
        self.engine = Pointer[None].get_null()

    fn open(inout self, wal_path: String, cas_path: String, policy: Int = 1):
        let open_fn = self.handle.get_function[
            fn(Pointer[Int8], Pointer[Int8], Pointer[UInt8], Int) -> Pointer[None]
        ]("isotime_open")
        
        let wal_ptr = Pointer[Int8](wal_path.as_bytes().as_ptr())
        let cas_ptr = Pointer[Int8](cas_path.as_bytes().as_ptr())
        
        self.engine = open_fn(wal_ptr, cas_ptr, Pointer[UInt8].get_null(), policy)

    fn get(self, key: String) -> String:
        if self.engine.is_null():
            return ""
            
        let get_fn = self.handle.get_function[
            fn(Pointer[None], Pointer[UInt8], Int) -> IsotimeBuffer
        ]("isotime_get")
        
        let free_fn = self.handle.get_function[
            fn(IsotimeBuffer) -> None
        ]("isotime_free_buffer")
        
        let key_bytes = key.as_bytes()
        let res_buffer = get_fn(self.engine, key_bytes.as_ptr(), len(key_bytes))
        
        if res_buffer.data.is_null():
            return ""
            
        # Convert buffer to String
        let result = String(res_buffer.data, res_buffer.len)
        
        # Free the buffer in Rust
        free_fn(res_buffer)
        
        return result

    fn close(inout self):
        if not self.engine.is_null():
            let close_fn = self.handle.get_function[
                fn(Pointer[None]) -> None
            ]("isotime_close")
            close_fn(self.engine)
            self.engine = Pointer[None].get_null()
