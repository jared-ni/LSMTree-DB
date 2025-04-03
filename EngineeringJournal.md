3/15:
- Used the cs165 src code
- Modified the client-server architecture to use multiplexing to handle multiple clients
- I could've also used thread pools, but multiplexing is simply a better design for many-clients in a database project

3/16: 
- cleaned up the code (a LOT), deleted all the src I don't need for now. 
- created the parser for ingesting client query.
- gonna start working on the actual LSM tree now. 

3/17: 
LSM Tree notes:
- LSM tree writes in memtable (usually balanced binary tree)
- memtable is flushed to disk when full as an immutable sorted string table called the SSTable. 
- SSTable stores key-value pairs in sorted. They are all sequential io. 
- As more data comes in, more immutable SSTable is created. 
- Update: 
    - updates to existing object key doesn't motify the SSTable, since it's immutable. 
    - new entry added to the newest SSTable, which supercedes the old SSTable. 
- delete:
    - adds a marker called a tombstone to the most recent SSTable of the object.
    - when see tombstone, we know it's deleted. 
    - overtime, this can cost disk space, so when we merge into next level, we disgard marked values. 
    - the merge is similar to merge in merge sort. 
- read:
    - try to find request in mem table, then sequentially in SSTables. 
    - SSTables are sorted so this can be done efficiently. 
    - To prevent read inefficiency, we have a limit on the SSTables per level, merge and flush to next level when we reach limit. 
- Optimizations: 
    - Tiering is write optimized, leveling is read-optimized
    - each level get exponentially larger. compaction tuning is the most critical for LSM tree.
    - read:
        - look up require looking through every SSTable. 
        - many keep summary table in memery that keeps min/max range of each disk block of every level, allows system to skip search on those ranges
        - look up keys that doesn't exist: bloom filter at each level to optimize, returns no if key doesn't exist, and probably yes if the key might exist. 

3/20: 
- L0 full -> compaction
- when compaction happens, all SSTables from L0 are selected, and all SSTables from L1 with overlapping range. 
- Merge sort all of these SSTables -> replaces SSTables in L1. 
- compaction from L1 to L2 only happens when L1 is full.

- each run is a SSTable
- Apparently slides descibe STCS, and level/tier now is used interchangeably. This is what I understood: when flushing, we flush all SSTables on current level into a single SSTable on the next level. 
- Ok, maybe in the context of fence pointers this makes sense. We have a level/run, and it includes multiple fence pointers that have non-overlapping ranges. 


Ok here's my design: 
- when flushing, merge all content of the current level to the next
- oh well, my current approach is leveling (not tiering described in the TF video). I thought it would be easier, so I'm gonna continue and see... I'll implement both eventually, hopefully. 

Leveling (LCS):
- L0 -> L1: take all tables
- L1 -> L(N+1): use one table from L1, find all tables in L(n+1) with overlapping key ranges with the Ln table. 
- merge: use min-heap to find smallest key across all input tables
- process smallest key
- handle tombstones
- output: 
    - write results (live or tombstones) to new output SSTable in N+1, 
    - key track of size in this output sstable
    - if reach target size, finalize it and start new output for rest of the merged data
    - remove original input tables from L and L+1, add new table to L+1
    - check if L+1 now can needs compact


Fence pointers: 
- Doesn't change how this works, is an internal look up optimization for SSTables

Bloom Filters: 
- 

3/24:
- Ok for persistence, I need: 
    - 1 disk file per SSTable
    - 1 folder per level 
    - track all changes in a log file so the data is recoverable

3/25: 
- Changed to tiering
- flush when we reach capacity, not when we exceed it, is much simpler

3/26:
- add fence pointers
    - I think it makes more sense for the SSTables to have fence pointers, since they are individual files
    - SSTables already have ranges, and adding fence pointers would make them more fine-grained. 
    - plus, SSTable data are immutable, thus making fence ptrs easier to use/construct. 
- add bloom filters
    - it also makes sense to have one bloom filter per SSTable/file, given that a number already fall into the range? 
    - reason for this: 
        - only check bloom filter if number falls into range, and SSTables are sorted
Together: 
- check each SSTable's range, then its Bloom filter
    - if hit, use fence pointers to find the block, then do binary search within the block

Last bit of work:
- integrate client-server with lsm tree
- range query
- design experiments:
    - size ratio? 
        1. get a workload generated
        2. plot the seconds it takes to run the workload
        3. do it 3-5 time on the same workload, with different size ratio
    - 

3/28: 
- comparison

3/31: 
- re-record the data, free up the max entry size this time. 
- size ratio 2 vs. 10 comparison. 

4/2:
- Implemented SSTable-level bloom filter, and it works
- For 1GB put 10MB get, it adds 50 sec to put, but keeps get at 3 seconds. 
- Constant I/O takes a lot of time. I will consider moving the bloom filter to Level, 
and have one bloom filter per level instead. 