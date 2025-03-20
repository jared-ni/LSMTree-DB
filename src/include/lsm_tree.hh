#ifndef LSM_TREE_HH
#define LSM_TREE_HH

#include <string>
#include <vector>
#include <memory>
#include <map>
#include <mutex>
#include <shared_mutex>

#define BUFFER_CAPACITY 10
#define BASE_LEVEL_TABLE_CAPACITY 5
#define LEVEL_SIZE_RATIO 10 // how much bigger l1 is than l0
#define MAX_LEVELS 7

class DataPair {
    public:
    DataPair(long key, long value, bool deleted = false);
    long key_;
    long value_;
    // tombstone to mark deleted keys
    bool deleted_;

    bool operator<(long other_key) const;
    bool operator<(const DataPair& other) const;
    bool operator==(const DataPair& other) const;
};


class SSTable {
    public:
    SSTable(const std::vector<DataPair>& data, 
                     int level_num);
                    //  const std::string& file_name);

    // std::string file_name_;
    int level_num_;

    long min_key_;
    long max_key_;
    size_t size_;

    std::vector<DataPair> table_data_;

    // check if Key is in range of SSTable
    bool keyInRange(long key) const;
    bool keyInSSTable(long key) const;
    std::optional<DataPair> getDataPair(long key) const;
};

// each level of the LSM Tree
class Level {
    public:
    // level_num is id, capacity is max size of this level, cur_size is current size
    Level(int level_num, size_t capacity, size_t cur_size = 0);
    int level_num_;
    size_t capacity_;
    size_t cur_size_; // number of SSTables in this level
    // tracks all SSTables on the current level
    std::vector<std::shared_ptr<SSTable>> sstables_;
    // probably need a mutex for later
    mutable std::shared_mutex mutex_;

    // SSTable methods
    // don't think raw ptr gon work; prevent dangling ptrs in multithreaded env
    const std::vector<std::shared_ptr<SSTable>>& getSSTables() const;
    // can't edit SSTables, can only add or remove
    void addSSTable(std::shared_ptr<SSTable> sstable_ptr);
    void removeSSTable(std::shared_ptr<SSTable> sstable_ptr);

    // compaction
    bool needsCompaction() const;

    void printLevel() const;
};

// buffer/memtable, where all data is stored before being flushed to disk
class Buffer {
    public:
    Buffer(size_t capacity = BUFFER_CAPACITY, size_t cur_size = 0);
    
    size_t capacity_;
    size_t cur_size_;
    // need to refactor to balanced binary tree, skip list, or B tree
    std::vector<DataPair> buffer_data_;

    bool isFull() const;
    std::shared_ptr<SSTable> flushBuffer();
    // API: put, get, range, delete
    bool putData(const DataPair& data);
    std::optional<DataPair> getData(long key) const;
    // std::vector<DataPair> getRangeData(long start, long end) const;
    // bool deleteData(long key);
};


class LSMTree {
    public:
    LSMTree(const std::string& db_path, 
            size_t buffer_capacity = BUFFER_CAPACITY,
            size_t base_level_table_capacity = BASE_LEVEL_TABLE_CAPACITY, 
            size_t total_levels = MAX_LEVELS, 
            size_t level_size_ratio = LEVEL_SIZE_RATIO);

    std::string db_path_;

    size_t buffer_capacity_;
    size_t base_level_table_capacity_;
    size_t total_levels_;
    size_t level_size_ratio_;
    
    std::unique_ptr<Buffer> buffer_;
    // LSM tree owns the levels, so unique_ptr, and it coordinates buffer/level flushes
    std::vector<std::unique_ptr<Level>> levels_;

    // buffer methods
    void flushBuffer();
    // API: put, get, range, delete
    bool putData(const DataPair& data);
    std::optional<DataPair> getData(long key) const;

    // protects all flush: only one thread can flush at a time
    std::mutex flush_mutex_;
};

#endif