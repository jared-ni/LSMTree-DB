**Plan of Attack**
Phase 1: LSM-tree core components
1. Integrate the data generator found at https://bitbucket.org/HarvardDASlab/cs265-sysproj/src/master/. 

2. Write the parser for the domain specific language of the DB queries. 

3. Buffer (L0) implementation: 
    - implement in-memory sorted data structure, like a skip list / balanced tree, to buffer writes (fast writes)
    - support *put*, *delete*, and in-memory *get* operations. 
    - serialize buffer to disk during flush (e.g. SSTable)

4. Create disk-level data management:
    - design file format for sorted runs (keys, values, min/max keys). 
    - define size ratios between levels (L1 = L0 * 10)
    - create manifest structure: describes the file to level relationships and indicate which set of files form a current snapshot

5. Handle merging / compaction
    - triggered when level reaches capacity, or set of sorted files overlapping (called run).
    - merge sorted run from level n to n+1 (can lead to cascading way).
    - write merged results to next level.
6. add bloom filter to each level with optimized bits per entry to determine if key is not contained in that level.
7. add fence pointers in each level to allow page or block access within a run. 

Phase 2: 
8. save manifest (in-memory and persisted)