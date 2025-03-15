**Plan of Attack**
Phase 1: LSM-tree core components
0. client-server arch: 
- https://code.harvard.edu/uts797/cs165-2024-starter-code/blob/master/src/server.c
- do it correctly for the queries, 
- load data in client, then send it to server
- some script that can simulate using docker
- or just do the server side first
- then parser, on client side (or maybe split it among client and server, but server side would be fine to start with)
- 

1. Integrate the data generator found at https://bitbucket.org/HarvardDASlab/cs265-sysproj/src/master/. 

2. Write the parser for the domain specific language of the DB queries. 

3. Buffer (L0) implementation: 
    - implement in-memory sorted data structure, like a skip list / balanced tree, to buffer writes (fast writes)   
        - begin with just log, skip list / blanaced tree would come later to optimize
        - buffer should just support the three functions
    - support *put*, *delete*, and in-memory *get* operations. 
    - serialize buffer to disk during flush (e.g. SSTable)
    - do leveling first, then tiering or lazy can come later
    - tiering: has different optimizations 

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

8. Multithreading:
    - start with writes multithread 
        - each thread takes a chunk of data, and process them
        - 2D design space: 
            - split your data, combine
            if multiple queries, now you can also choose query. How to split threads among query and data? 2D design space
            one thread take care one query, or each thread take care of 
    Two cases:
    - only 1 query: multiplethread single query
    - multiple query: 

    - Start with one client with bunch of queries, then go multiple clients with bunch of queries

Phase 2: 
8. save manifest (in-memory and persisted)


Monkey: 
- optimizing best size for bloom filter, size of memory for each level
Doev: 
- best merging policy: after you have best bloom filter, what's the best merging policy


1. basic (server-client, log for buffer, merging, multithreading)
2. 4-5 design choices, experimental results. 8 experiments is good. 
3. make code private, then give TF read access

Before midway check-in: 
- client-server, all the basic things
- 2 experiments

final report will be strict: 
- make your best effort on the midway check-in

3/14 Meet with Stratos:
Midway checkin experiments:
- Look at Monkey how to design experiments. 
- Test with 10, 100, 10000, 1 mil, etc. are the experiments. Start with default hyperparameters
- multithreading:
    - reasonable decision to begin with,
    - next, maybe try something else like 
    - 

