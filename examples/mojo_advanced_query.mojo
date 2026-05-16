from isotime import Isotime
import os

fn main() raises:
    print("Mojo Advanced Query Demo")
    
    # Path to the compiled Rust library
    let lib_path = "./target/debug/libisotime.so"
    
    var db = Isotime(lib_path)
    db.open("mojo_query.wal", "mojo_cas")
    
    print("Putting sample data...")
    # Put sample numeric data
    db.put("m1", "value_1") # We need a way to pass f64 as bytes in Mojo easily for this to match .range()
    # Actually, for range to work, the value MUST be 8 bytes (f64).
    # In Mojo, we can bitcast f64 to 8 bytes.
    
    print("Executing query...")
    let query = db.query()
    query.tag("iot")
    
    let results = query.execute()
    
    print("Query execution completed.")
    
    db.close()
    print("Demo finished.")
