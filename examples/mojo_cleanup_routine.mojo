from isotime import Isotime, ISOTIME_OK
import os

fn main() raises:
    print("Mojo Full Life-cycle Cleanup Demo")
    
    # Path to the compiled Rust library
    let lib_path = "./target/debug/libisotime.so"
    
    var db = Isotime(lib_path)
    db.open("cleanup.wal", "cleanup_cas")
    
    print("1. Putting data...")
    db.put("temp_data", "to_be_deleted")
    
    print("2. Deleting data...")
    let del_status = db.delete("temp_data")
    if del_status == ISOTIME_OK:
        print("   -> OK: Data marked for deletion.")
    else:
        print("   -> Error: Delete failed.")
        
    print("3. Flushing to disk...")
    let flush_status = db.flush()
    if flush_status == ISOTIME_OK:
        print("   -> OK: MemTable persisted to SSTable.")
        
    print("4. Running Garbage Collection...")
    let gc_status = db.run_gc()
    if gc_status == ISOTIME_OK:
        print("   -> OK: Orphaned CAS objects reclaimed.")
        
    db.close()
    print("\nCleanup demo finished successfully.")
