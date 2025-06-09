from kvstore.hash_ring import HashRing

def main():
    print("=== Testing Hash Ring ===")
    
    ring = HashRing()
    
    ring.add_node("server1")
    ring.add_node("server2") 
    ring.add_node("server3")
    
    test_keys = ["user1", "user2", "user3", "data1", "data2", "session123"]
    
    print("\nKey distribution:")
    for key in test_keys:
        node = ring.get_node(key)
        print(f"Key '{key}' -> {node}")
    
    print(f"\nBefore removal - all nodes: {ring.get_all_nodes()}")
    ring.remove_node("server2")
    print(f"After removing server2 - nodes: {ring.get_all_nodes()}")
    
    print("\nKey distribution after server2 removed:")
    for key in test_keys:
        node = ring.get_node(key)
        print(f"Key '{key}' -> {node}")

if __name__ == "__main__":
    main()