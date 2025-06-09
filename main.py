from kvstore.simple_store import SimpleKVStore

def main():
    print("=== Testing Basic KV Store ===")
    
    store = SimpleKVStore()
    
    # Test putting data
    print("Storing some data...")
    store.put("user:alice", "Alice Johnson")
    store.put("user:bob", "Bob Smith")
    store.put("counter", "42")
    
    # Test getting data
    print(f"user:alice = {store.get('user:alice')}")
    print(f"user:bob = {store.get('user:bob')}")
    print(f"counter = {store.get('counter')}")
    
    # Test non-existent key
    print(f"missing_key = {store.get('missing_key')}")
    
    # Test size and keys
    print(f"Total keys: {store.size()}")
    print(f"All keys: {store.keys()}")
    
    # Test deletion
    print(f"Deleting 'counter': {store.delete('counter')}")
    print(f"Deleting 'missing_key': {store.delete('missing_key')}")
    print(f"Keys after deletion: {store.keys()}")
    print(f"Size after deletion: {store.size()}")

if __name__ == "__main__":
    main()