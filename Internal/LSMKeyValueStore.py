from Application.Internal.MemTable import MemTable
from Application.Internal.SSTable import SSTable, SSTableBF

class LSMKeyValueStore:
    def __init__(self, mem_table_size_threshold=3, sstable_size_threshold=5, max_levels=2, sstable_factor=2):
        self.memtable = MemTable()
        self.levels = [SSTable() for _ in range(max_levels)]
        self.mem_table_size_threshold = mem_table_size_threshold
        self.sstable_size_threshold = sstable_size_threshold
        self.max_levels = max_levels
        self.sstable_factor = sstable_factor

    def put(self, key, value):
        self.memtable.put(key, value)
        if len(self.memtable.entries) >= self.mem_table_size_threshold:
            self.flush_memtable()

    def get(self, key):
        result = self.memtable.get(key)
        if result is not None:
            return result

        for level in reversed(self.levels):
            result = level.get(key)
            if result is not None:
                return result

        return None
    
    def delete(self, key):
        # Mark the key as deleted in the memtable
        self.memtable.put(key, None)
        # The None value serves as a tombstone, indicating that the key has been deleted.
        
        # Check if the memtable has reached the specified size threshold, then flush to disk
        if len(self.memtable.entries) >= self.mem_table_size_threshold:
            self.flush_memtable()

    def flush_memtable(self):
        sstable = SSTable()
        sstable.entries = list(self.memtable.entries)
        self.memtable = MemTable()

        # Merge SSTables without adjusting sizes for the first level
        self.levels[0] = self.merge_sstables(self.levels[0], sstable)

        # Calculate the adjusted size for the first level SSTable
        adjusted_size_level_0 = int(self.sstable_size_threshold * (self.sstable_factor ** 0))

        # Adjust the size of the first level SSTable if needed
        if len(self.levels[0].entries) > adjusted_size_level_0:
            self.levels[0], overflow = self.split_sstable(self.levels[0], adjusted_size_level_0)
            self.levels[1] = self.merge_sstables(self.levels[1], overflow)

        for i in range(1, len(self.levels)):
            # Calculate the adjusted size for the current level SSTable
            adjusted_size = int(self.sstable_size_threshold * (self.sstable_factor ** i))

            # Split the current level SSTable if needed
            if len(self.levels[i - 1].entries) >= adjusted_size:
                self.levels[i - 1], overflow = self.split_sstable(self.levels[i - 1], adjusted_size)
                self.levels[i] = self.merge_sstables(self.levels[i], overflow)

    def merge_sstables(self, sstable1, sstable2):
        merged = SSTable()

        # Merge sorted entries, handling None values separately
        entries_sstable1 = [(entry[0], entry[1]) for entry in sstable1.entries if entry[0] is not None]
        entries_sstable2 = [(entry[0], entry[1]) for entry in sstable2.entries if entry[0] is not None]

        merged.entries = sorted(
            entries_sstable1 + entries_sstable2,
            key=lambda x: (x[0] is None, x[0])
        )

        return merged
        # merged = SSTable()
        # merged.entries = sorted(sstable1.entries + sstable2.entries)
        # return merged

    def split_sstable(self, sstable, target_size):
        mid = len(sstable.entries) // 2

        # Ensure that both left and right SSTables have at least target_size entries
        while mid > 0 and mid < len(sstable.entries) - 1:
            left = SSTable()
            left.entries = sstable.entries[:mid]
            right = SSTable()
            right.entries = sstable.entries[mid:]

            if len(left.entries) >= target_size and len(right.entries) >= target_size:
                return left, right

            # Adjust mid for the next iteration
            mid = mid - 1

        # If we can't split into two SSTables with at least target_size entries, return the original SSTable
        return sstable, SSTable()


    def get_levels_sizes(self):
        sizes = [len(level.entries) for level in self.levels]
        return sizes
    
class LSMKeyValueStoreBF:
    def __init__(self, mem_table_size_threshold=3, sstable_size_threshold=5, max_levels=2, sstable_factor=2):
        self.memtable = MemTable()
        self.levels = [SSTableBF() for _ in range(max_levels)]
        self.mem_table_size_threshold = mem_table_size_threshold
        self.sstable_size_threshold = sstable_size_threshold
        self.max_levels = max_levels
        self.sstable_factor = sstable_factor

    def put(self, key, value):
        self.memtable.put(key, value)
        if len(self.memtable.entries) >= self.mem_table_size_threshold:
            self.flush_memtable()

    def get(self, key):
        # if not self.memtable.might_contain(key):
        #     return None
        result = self.memtable.get(key)
        if result is not None:
            return result

        for level in reversed(self.levels):
            if not level.bloom_filter.might_contain(key):
                continue

            result = level.get(key)
            if result is not None:
                return result

        return None
    
    def delete(self, key):
        # Mark the key as deleted in the memtable
        self.memtable.put(key, None)
        # The None value serves as a tombstone, indicating that the key has been deleted.
        
        # Check if the memtable has reached the specified size threshold, then flush to disk
        if len(self.memtable.entries) >= self.mem_table_size_threshold:
            self.flush_memtable()

    def flush_memtable(self):
        sstable = SSTableBF()
        sstable.entries = list(self.memtable.entries)
        self.memtable = MemTable()

        # Merge SSTables without adjusting sizes for the first level
        self.levels[0] = self.merge_sstables(self.levels[0], sstable)

        # Calculate the adjusted size for the first level SSTable
        adjusted_size_level_0 = int(self.sstable_size_threshold * (self.sstable_factor ** 0))

        # Adjust the size of the first level SSTable if needed
        if len(self.levels[0].entries) > adjusted_size_level_0:
            self.levels[0], overflow = self.split_sstable(self.levels[0], adjusted_size_level_0)
            self.levels[1] = self.merge_sstables(self.levels[1], overflow)

        for i in range(1, len(self.levels)):
            # Calculate the adjusted size for the current level SSTable
            adjusted_size = int(self.sstable_size_threshold * (self.sstable_factor ** i))

            # Split the current level SSTable if needed
            if len(self.levels[i - 1].entries) >= adjusted_size:
                self.levels[i - 1], overflow = self.split_sstable(self.levels[i - 1], adjusted_size)
                self.levels[i] = self.merge_sstables(self.levels[i], overflow)

    def merge_sstables(self, sstable1, sstable2):
        merged = SSTableBF()

        # Merge sorted entries, handling None values separately
        entries_sstable1 = [(entry[0], entry[1]) for entry in sstable1.entries if entry[0] is not None]
        entries_sstable2 = [(entry[0], entry[1]) for entry in sstable2.entries if entry[0] is not None]

        merged.entries = sorted(
            entries_sstable1 + entries_sstable2,
            key=lambda x: (x[0] is None, x[0])
        )

        return merged
        # merged = SSTable()
        # merged.entries = sorted(sstable1.entries + sstable2.entries)
        # return merged

    def split_sstable(self, sstable, target_size):
        mid = len(sstable.entries) // 2

        # Ensure that both left and right SSTables have at least target_size entries
        while mid > 0 and mid < len(sstable.entries) - 1:
            left = SSTableBF()
            left.entries = sstable.entries[:mid]
            right = SSTableBF()
            right.entries = sstable.entries[mid:]

            if len(left.entries) >= target_size and len(right.entries) >= target_size:
                return left, right

            # Adjust mid for the next iteration
            mid = mid - 1

        # If we can't split into two SSTables with at least target_size entries, return the original SSTable
        return sstable, SSTableBF()


    def get_levels_sizes(self):
        sizes = [len(level.entries) for level in self.levels]
        return sizes