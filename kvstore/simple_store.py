class SimpleKVStore:
    def __init__(self):
        self.data = {}
    
    def get(self, key):
        return self.data.get(key)
    
    def put(self, key, value):
        self.data[key] = value

    def delete(self, key):
        if key in self.data:
            del self.data[key]
            return True
        return False

    def size(self):
        return len(self.data)
    
    def keys(self):
        return list(self.data.keys())