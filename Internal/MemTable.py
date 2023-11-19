import bisect

class MemTable:
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
    