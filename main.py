import random
import sys
import time

sys.path.append('../')

from Application.Internal.LSMKeyValueStore import LSMKeyValueStore, LSMKeyValueStoreBF
from Application.Internal.Constants import Constants

class Stats: 
    put_operations=0
    get_operations=0
    delete_operations = 0
    cummulative_put_time = 0
    cummulative_get_time = 0
    cummulative_delete_time = 0

class RandomString:
    upper = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
    lower = upper.lower()
    digits = "0123456789"
    alphanum = upper + lower + digits

    @staticmethod
    def next_string(random: random.Random, length: int) -> str:
        return ''.join(random.choice(RandomString.alphanum) for _ in range(length))

def get_next_key(random: random.Random) -> str:
    return RandomString.next_string(random, 10)

def initializeDatabase(rng : int, db : LSMKeyValueStore) -> None:
    for i in range(rng):    
        key = get_next_key(random)
        key = key[0:2] #cutting the size of key 
        value = random.randint(0, int(10e1))
        db.put(key, value)
        print(f"{i+1} PUT: {key},{value}")

def generate_command(i: int, random: random.Random, db: LSMKeyValueStore, stats : Stats, consts: Constants) -> None:
    key = get_next_key(random)
    key = key[0:2] #cutting the size of key 
    j = i + 1

    operations = [0,1,2]
    probabilities = consts.probability  # Adjust these probabilities accordingly
    # get, put, delete
    choice = random.choices(operations, weights=probabilities)[0]

    start_time = time.time()

    if choice == 0:
        stats.get_operations += 1
        opt = db.get(key)
        if opt is not None:
            pass
            # print(f"{j} GET: {opt} ____________________> found success")
        else:
            pass
            # print(f"{j} GET: No Such Element {key} ")

    elif choice == 1:
        stats.put_operations += 1
        value = random.randint(0, int(10e1))
        db.put(key, value)
        # print(f"{j} PUT: {key},{value}")

    elif choice == 2:
        stats.delete_operations += 1
        # print(f"{j} DELETE: {key} but not implemented")
        if db.delete(key) == 0:
            pass
            # print(f"{j} DELETE: {key}")
        else:
            pass
            # print(f"{j} DELETE: No Such Element")

    end_time = time.time()
    elapsed_time = end_time - start_time

    if choice == 0:
        stats.cummulative_get_time += elapsed_time
    elif choice == 1:
        stats.cummulative_put_time += elapsed_time
    elif choice == 2: 
        stats.cummulative_delete_time += elapsed_time

def main():
    consts = Constants()
    stats = Stats()
    r = random.Random()
    
    # kv_store = LSMKeyValueStore(consts.mem_table_size_threshold,
    #                             consts.sstable_size_threshold,
    #                             consts.max_levels,
    #                             consts.sstable_factor)
    

    kv_store = LSMKeyValueStoreBF(consts.mem_table_size_threshold,
                                consts.sstable_size_threshold,
                                consts.max_levels,
                                consts.sstable_factor)
    
    initializeDatabase(consts.initDbSize, kv_store)

    # putOperations = initDbSize + putOperations

    for f in range(consts.transactions):   
        generate_command(f,r,kv_store,stats,consts)

    print("_"*30)
    print("Total puts = ",stats.put_operations)
    print("Total gets = ",stats.get_operations)
    print("Total deletes = ",stats.delete_operations)
    print("_"*30)
    print("puts time = ",stats.cummulative_put_time)
    print("gets time = ",stats.cummulative_get_time)
    print("deletes time = ",stats.cummulative_delete_time)
    print("_"*30)
    
    print("puts throughput = ", 0 if stats.put_operations==0 else stats.cummulative_put_time/stats.put_operations)
    print("gets throughput = ", 0 if stats.get_operations==0 else stats.cummulative_get_time/stats.get_operations)
    print("deletes throughput = ", 0 if stats.delete_operations==0 else stats.cummulative_delete_time/stats.delete_operations)
    print("_"*30)
    print("MemTable : ", len(kv_store.memtable.entries) )
    levels_sizes = kv_store.get_levels_sizes()
    for i, size in enumerate(levels_sizes):
        print(f"SS Lvl {i} : {size} entries")

    
if __name__=="__main__":
    main()