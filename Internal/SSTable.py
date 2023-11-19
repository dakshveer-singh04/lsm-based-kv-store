import bisect
from Application.Internal.BloomFilter import BloomFilter

class SSTable:
    def __init__(self):
        self.entries = []

    def put(self, key, value):
        index = bisect.bisect_left(self.entries, (key,))
        self.entries.insert(index, (key, value))

    def get(self, key):
        index = bisect.bisect_left(self.entries, (key,))
        if index < len(self.entries) and self.entries[index][0] == key:
            return self.entries[index][1]
        return None
    
class SSTableBF:
    def __init__(self, bloom_filter_size=100, bloom_filter_hash_functions=3):
        self.entries = []
        self.bloom_filter_size = bloom_filter_size
        self.bloom_filter_hash_functions = bloom_filter_hash_functions
        self.bloom_filter = BloomFilter(bloom_filter_size, bloom_filter_hash_functions)

    def put(self, key, value):
        self.entries.append((key, value))
        self.bloom_filter.add(key)

    def get(self, key):
        if not self.bloom_filter.might_contain(key):
            return None

        for entry in self.entries:
            if entry[0] == key:
                return entry[1]

        return None
    
