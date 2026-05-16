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

@value
struct IsotimeEntry(CollectionElement):
    var key_data: Pointer[UInt8]
    var key_len: Int
    var val_data: Pointer[UInt8]
    var val_len: Int

    fn __init__(inout self, key_data: Pointer[UInt8], key_len: Int, val_data: Pointer[UInt8], val_len: Int):
        self.key_data = key_data
        self.key_len = key_len
        self.val_data = val_data
        self.val_len = val_len

    fn key(self) -> String:
        if self.key_data.is_null():
            return ""
        return String(self.key_data, self.key_len)

    fn value(self) -> String:
        if self.val_data.is_null():
            return ""
        return String(self.val_data, self.val_len)

    fn value_as_f64(self) -> Float64:
        if self.val_len != 8 or self.val_data.is_null():
            return 0.0
        return self.val_data.bitcast[Float64]().load()

struct ResultSet:
    var handle: DLHandle
    var ptr: Pointer[None]

    fn __init__(inout self, handle: DLHandle, ptr: Pointer[None]):
        self.handle = handle
        self.ptr = ptr

    fn next(inout self) -> Optional[IsotimeEntry]:
        if self.ptr.is_null():
            return None
        
        let next_fn = self.handle.get_function[
            fn(Pointer[None]) -> IsotimeEntry
        ]("isotime_result_next")
        
        let entry = next_fn(self.ptr)
        if entry.key_data.is_null():
            return None
            
        return entry

    fn __del__(owned self):
        if not self.ptr.is_null():
            let free_fn = self.handle.get_function[
                fn(Pointer[None]) -> None
            ]("isotime_result_free")
            free_fn(self.ptr)

struct Query:
    var handle: DLHandle
    var ptr: Pointer[None]

    fn __init__(inout self, handle: DLHandle, ptr: Pointer[None]):
        self.handle = handle
        self.ptr = ptr

    fn tag(inout self, tag_name: String):
        let tag_fn = self.handle.get_function[
            fn(Pointer[None], Pointer[Int8]) -> None
        ]("isotime_query_tag")
        tag_fn(self.ptr, Pointer[Int8](tag_name.as_bytes().as_ptr()))

    fn range(inout self, min_val: Float64, max_val: Float64):
        let range_fn = self.handle.get_function[
            fn(Pointer[None], Float64, Float64) -> None
        ]("isotime_query_range")
        range_fn(self.ptr, min_val, max_val)

    fn after(inout self, clock: DynamicVector[UInt64]):
        let after_fn = self.handle.get_function[
            fn(Pointer[None], Pointer[UInt64], Int) -> None
        ]("isotime_query_after")
        after_fn(self.ptr, clock.data(), len(clock))

    fn execute(owned self) -> ResultSet:
        let exec_fn = self.handle.get_function[
            fn(Pointer[None]) -> Pointer[None]
        ]("isotime_query_execute")
        let res_ptr = exec_fn(self.ptr)
        return ResultSet(self.handle, res_ptr)

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

    fn put(self, key: String, value: String):
        if self.engine.is_null():
            return
        let put_fn = self.handle.get_function[
            fn(Pointer[None], Pointer[UInt8], Int, Pointer[UInt8], Int) -> None
        ]("isotime_put")
        let key_bytes = key.as_bytes()
        let val_bytes = value.as_bytes()
        put_fn(self.engine, key_bytes.as_ptr(), len(key_bytes), val_bytes.as_ptr(), len(val_bytes))

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
            
        let result = String(res_buffer.data, res_buffer.len)
        free_fn(res_buffer)
        return result

    fn query(self) -> Query:
        let query_new_fn = self.handle.get_function[
            fn(Pointer[None]) -> Pointer[None]
        ]("isotime_query_new")
        let query_ptr = query_new_fn(self.engine)
        return Query(self.handle, query_ptr)

    fn close(inout self):
        if not self.engine.is_null():
            let close_fn = self.handle.get_function[
                fn(Pointer[None]) -> None
            ]("isotime_close")
            close_fn(self.engine)
            self.engine = Pointer[None].get_null()
