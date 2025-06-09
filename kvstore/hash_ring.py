import hashlib

class HashRing:
    def __init__(self):
        self.nodes = {}  # hash_position -> node_id
        self.sorted_hashes = []
    
    def _hash(self, key):
        return int(hashlib.md5(key.encode()).hexdigest(), 16)
    
    def add_node(self, node_id):
        node_hash = self._hash(node_id)
        self.nodes[node_hash] = node_id
        self.sorted_hashes = sorted(self.nodes.keys())
        print(f"Added node {node_id} at position {node_hash}")
    
    def remove_node(self, node_id):
        node_hash = self._hash(node_id)
        if node_hash in self.nodes:
            del self.nodes[node_hash]
            self.sorted_hashes = sorted(self.nodes.keys())
            print(f"Removed node {node_id}")
    
    def get_node(self, key):
        nodes = self.get_nodes(key, replicas=1)
        return nodes[0] if nodes else None
    
    def get_nodes(self, key, replicas=3):
        if not self.nodes:
            return []
        
        key_hash = self._hash(key)
        
        start_idx = 0
        for i, node_hash in enumerate(self.sorted_hashes):
            if node_hash >= key_hash:
                start_idx = i
                break
        result = []
        seen_nodes = set()
        
        for i in range(len(self.sorted_hashes)):
            idx = (start_idx + i) % len(self.sorted_hashes)
            node_hash = self.sorted_hashes[idx]
            node_id = self.nodes[node_hash]
            
            if node_id not in seen_nodes and len(result) < replicas:
                result.append(node_id)
                seen_nodes.add(node_id)
            
            if len(result) >= replicas or len(seen_nodes) >= len(self.nodes):
                break
        
        return result
    
    def get_all_nodes(self):
        return list(self.nodes.values())