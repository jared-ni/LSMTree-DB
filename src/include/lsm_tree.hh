#ifndef LSM_TREE_HH
#define LSM_TREE_HH

#include <string>
#include <vector>
#include <memory>
#include <map>
#include <mutex>
#include <shared_mutex>
#include <thread>
#include <queue>
// persistence
#include <filesystem>
#include <sstream>
#include <iomanip>
#include <atomic>
#include <fstream>
#include <unistd.h>
#include <sys/types.h>
#include "bloom_filter.hh"


#define BUFFER_CAPACITY 100
#define BASE_LEVEL_TABLE_CAPACITY 2
#define LEVEL_SIZE_RATIO 2 // how much bigger l1 is than l0
#define MAX_LEVELS 10
// #define MAX_ENTRIES_PER_LEVEL 5120000000000
#define MAX_TABLE_SIZE 1000000
#define FENCE_PTR_BLOCK_SIZE 170 // 4096 / (12 * 2) = 170 bytes

// DataPair is 12 bytes
// 10MB = 10485760 Bytes = 873,814 DataPairs
class DataPair {
    public:
    DataPair(int key, int value, bool deleted = false);
    int key_;
    int value_;
    // tombstone to mark deleted keys
    bool deleted_;

    bool operator<(int other_key) const;
    bool operator<(const DataPair& other) const;
    bool operator==(const DataPair& other) const;
};

struct fence_ptr {
    int min_key;
    // where the block starts in the file
    // size_t file_offset; 
    size_t data_offset;
    size_t block_size_actual_;
};

// snapshots of the states
struct SSTableSnapshot {
    int level_num;
    int min_key;
    int max_key;
    size_t size;
    std::vector<DataPair> table_data;

    void printSSTable() const;
};
struct LevelSnapshot {
    int level_num;
    size_t table_capacity;
    // size_t entries_capacity;
    size_t current_table_count;
    size_t current_total_entries;
    std::vector<SSTableSnapshot> sstables;

    void printLevel() const;
};

class SSTable {
    public:
    SSTable(const std::vector<DataPair>& data, int level_num, 
            const std::string& file_path, const std::string& bf_file_path);
    // prepare for log loading
    SSTable(int level_num, const std::string& file_path, 
            const std::string& bf_file_path);

    std::string file_path_;
    std::string bf_file_path_;

    int level_num_;

    int min_key_;
    int max_key_;
    size_t size_;

    // TODO: bloom filter
    BloomFilter bloom_filter_;

    // TODO: array of fence pointers for binary search on each block
    std::vector<fence_ptr> fence_pointers_;
    // size of each fence pointer block, used for binary search
    size_t fence_pointer_block_size_ = FENCE_PTR_BLOCK_SIZE;

    // persistence:
    // maybe use lazy-loading, only load data when needed
    std::vector<DataPair> table_data_;
    bool data_loaded_;

    // TODO: synchronization: add a mutex to protect lazy loading in loadFromDisk
    mutable std::mutex sstable_mutex_;

    std::optional<std::pair<size_t, size_t>> getFenceRange(int key) const;

    bool writeToDisk() const;
    bool loadFromDisk();
    // end persistence

    void printSSTable() const;

    // check if Key is in range of SSTable
    bool keyInRange(int key) const;
    bool keyInSSTable(int key);
    std::optional<DataPair> getDataPair(int key);

};

// each level of the LSM Tree
class Level {
    public:
    // level_num is id, capacity is max size of this level, cur_size is current size
    Level(int level_num, 
          size_t table_capacity);
        //   size_t entries_capacity);

    int level_num_;

    size_t table_capacity_;
    // size_t entries_capacity_;

    size_t cur_table_count_;
    size_t cur_total_entries_;

    // tracks all SSTables on the current level
    std::vector<std::shared_ptr<SSTable>> sstables_;

    // concurrency protection: shared mutex for read
    // mutable std::mutex level_mutex_;
    mutable std::shared_mutex level_mutex_;

    // SSTable methods
    // don't think raw ptr gon work; prevent dangling ptrs in multithreaded env
    std::vector<std::shared_ptr<SSTable>> getSSTables() const;

    void addSSTable(std::shared_ptr<SSTable> sstable_ptr);
    void removeSSTable(std::shared_ptr<SSTable> sstable_ptr);
    void removeAllSSTables(const std::vector<std::shared_ptr<SSTable>>& tables_to_remove);

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

    // add concurrency protection for buffer synchronization
    // mutable std::mutex buffer_mutex_;
    // add shared mutex, so read can acquire shared lock, while write acquires exclusive locks
    mutable std::shared_mutex buffer_mutex_;

    void printBuffer() const;
    bool isFull() const;
    std::shared_ptr<SSTable> flushBuffer();
    // API: put, get, range, delete
    bool putData(const DataPair& data);
    std::optional<DataPair> getData(int key) const;
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
    ~LSMTree();
    void shutdown();

    // directory with levels that are folders
    std::string db_path_;

    std::string history_path_;
    std::atomic<uint64_t> next_file_id_{1};

    size_t buffer_capacity_;
    size_t base_level_table_capacity_;
    size_t total_levels_;
    size_t level_size_ratio_;
    
    std::unique_ptr<Buffer> buffer_;
    // LSM tree owns the levels, so unique_ptr, and it coordinates buffer/level flushes
    std::vector<std::unique_ptr<Level>> levels_;

    // LSMTree-level locks are for coordinating structural changes
    // the put/get/delete are handled by Buffer/Level/SSTable locks
    std::atomic<bool> shutdown_requested_{false};

    // -- make flush multithreaded --
    std::mutex flush_mutex_;
    std::condition_variable flush_request_cv_;
    bool flush_needed_{false};
    // automatic joining using jthread
    std::thread flusher_thread_;

    // flush logic ran by flusher thread
    void flushBufferHelper();
    void flushThreadLoop();
    void flushBuffer();

    // -- compaction multithreaded --
    std::mutex compaction_mutex_;
    std::condition_variable compaction_task_cv_;
    // queue of level indices that need compaction
    // std::queue<size_t> compaction_tasks_;
    // min-heap in c++, so we can pop the smallest level index first
    std::priority_queue<size_t, std::vector<size_t>, 
                        std::greater<size_t>> compaction_tasks_;
    std::thread compactor_thread_;

    void compactThreadLoop();
    void compactLevelHelper(size_t level_index);
    void doCompactionCheck(size_t level_index);

    // compaction logic
    bool checkCompaction(size_t level_index);
    void compactLevel(size_t level_index);
    // leveling merge policy main logic: 
    std::vector<std::shared_ptr<SSTable>> mergeSSTables(
        const std::vector<std::shared_ptr<SSTable>>& cur_level_tables,
        const std::vector<std::shared_ptr<SSTable>>& next_level_tables,
        int output_level_num);

    // API: put, get, range, delete
    bool putData(const DataPair& data);
    std::optional<DataPair> getData(int key);
    std::vector<DataPair> rangeData(int low, int high);
    bool deleteData(int key);

    // set up DB directory and history
    void setupDB();
    void loadHistory();
    void updateHistory(const std::vector<std::shared_ptr<SSTable>>& to_remove_l,
                       const std::vector<std::shared_ptr<SSTable>>& to_remove_l_next,
                       const std::vector<std::shared_ptr<SSTable>>& to_add_l_next);
    void updateHistoryAdd(std::shared_ptr<SSTable> new_sstable);
    std::string getLevelPath(int level_num) const;
    std::string getFilePath(int level_num, int file_id) const;
    std::string getBloomFilterPath(int level_num, int file_id) const;
    // delete physical file of an SSTable
    void deleteSSTableFile(const std::shared_ptr<SSTable>& sstable);

    // for testing
    std::vector<LevelSnapshot> getLevelsSnapshot() const;

    // for the print stats s command
    std::string print_stats();
};

struct MergeEntry {
    DataPair data;
    size_t source_table_index;
    size_t source_level_num;

    bool operator>(const MergeEntry& other) const {
        if (data.key_ != other.data.key_) {
            return data.key_ > other.data.key_;
        }
        // prioritize the one from the lower level, min_heap so higher is later
        return source_level_num > other.source_level_num;
    }
};

struct RangeEntry {
    DataPair data;
    int source_level;
    bool operator>(const RangeEntry& other) const {
        if (data.key_ != other.data.key_) {
            return data.key_ > other.data.key_;
        }
        return source_level > other.source_level;
    }
};

#endif