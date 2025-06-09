from kvstore.hash_ring import HashRing

def test_replication():
    print("=== Testing Replication Logic ===")
    
    ring = HashRing()
    
    ring.add_node("node1")
    ring.add_node("node2") 
    ring.add_node("node3")
    
    test_keys = ["user1", "user2", "user3", "data1"]
    
    print("\nWith 3 replicas (all nodes):")
    for key in test_keys:
        nodes = ring.get_nodes(key, replicas=3)
        print(f"Key '{key}' -> {nodes}")
    
    print("\nWith 2 replicas:")
    for key in test_keys:
        nodes = ring.get_nodes(key, replicas=2)
        print(f"Key '{key}' -> {nodes}")
    
    print("\nWith 1 replica (original behavior):")
    for key in test_keys:
        nodes = ring.get_nodes(key, replicas=1)
        print(f"Key '{key}' -> {nodes}")
    
    ring.add_node("node4")
    ring.add_node("node5")
    
    print(f"\nAfter adding more nodes (total: {len(ring.get_all_nodes())}):")
    print("With 3 replicas:")
    for key in test_keys[:2]:
        nodes = ring.get_nodes(key, replicas=3)
        print(f"Key '{key}' -> {nodes}")

if __name__ == "__main__":
    test_replication()