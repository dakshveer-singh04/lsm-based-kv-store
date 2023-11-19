class BloomFilter:
    def __init__(self, size, hash_functions):
        self.size = size
        self.hash_functions = hash_functions
        self.bit_array = [False] * size

    def add(self, key):
        for i in range(self.hash_functions):
            index = hash(key + str(i)) % self.size
            self.bit_array[index] = True

    def might_contain(self, key):
        for i in range(self.hash_functions):
            index = hash(key + str(i)) % self.size
            if not self.bit_array[index]:
                return False
        return True