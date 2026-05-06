from bindings.mojo.isotime import Isotime
import os

fn main() raises:
    print("Mojo: Connecting to Isotime Storage Engine...")
    
    # Path to the compiled Rust library
    let lib_path = "./target/debug/libisotime.so"
    
    var db = Isotime(lib_path)
    db.open("isotime.wal", "cas_store", 1) # 1 = Balanced policy
    
    # In a real scenario, data would be put via Rust or another interface
    # Here we just demonstrate the API call
    let result = db.get("test_key")
    
    if result == "":
        print("Mojo: Key not found (expected in this demo)")
    else:
        print("Mojo: Retrieved value ->", result)
    
    db.close()
    print("Mojo: Connection closed gracefully.")
