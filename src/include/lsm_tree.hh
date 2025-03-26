#ifndef LSM_TREE_HH
#define LSM_TREE_HH

#include <string>
#include <vector>
#include <memory>
#include <map>
#include <mutex>
#include <shared_mutex>
// persistence
#include <filesystem>
#include <sstream>
#include <iomanip>
#include <atomic>
#include <fstream>

#define BUFFER_CAPACITY 10
#define BASE_LEVEL_TABLE_CAPACITY 5
#define LEVEL_SIZE_RATIO 10 // how much bigger l1 is than l0
#define MAX_LEVELS 7
#define MAX_ENTRIES_PER_LEVEL 512

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

// snapshots of the states
struct SSTableSnapshot {
    int level_num;
    long min_key;
    long max_key;
    size_t size;
    std::vector<DataPair> table_data;

    void printSSTable() const;
};
struct LevelSnapshot {
    int level_num;
    size_t table_capacity;
    size_t entries_capacity;
    size_t current_table_count;
    size_t current_total_entries;
    std::vector<SSTableSnapshot> sstables;

    void printLevel() const;
};

class SSTable {
    public:
    SSTable(const std::vector<DataPair>& data, int level_num, 
            const std::string& file_name);
    // prepare for log loading
    SSTable(int level_num, const std::string& file_path);

    std::string file_path_;

    int level_num_;

    long min_key_;
    long max_key_;
    size_t size_;

    // persistence:
    // maybe use lazy-loading, only load data when needed
    std::vector<DataPair> table_data_;
    bool data_loaded_;

    bool writeToDisk() const;
    bool loadFromDisk();
    // end persistence

    void printSSTable() const;

    // check if Key is in range of SSTable
    bool keyInRange(long key) const;
    bool keyInSSTable(long key);
    std::optional<DataPair> getDataPair(long key);
};

// each level of the LSM Tree
class Level {
    public:
    // level_num is id, capacity is max size of this level, cur_size is current size
    Level(int level_num, 
          size_t table_capacity,
          size_t entries_capacity);

    int level_num_;

    size_t table_capacity_;
    size_t entries_capacity_;

    size_t cur_table_count_;
    size_t cur_total_entries_;

    // tracks all SSTables on the current level
    std::vector<std::shared_ptr<SSTable>> sstables_;
    // probably need a mutex for later
    mutable std::shared_mutex mutex_;

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

    void printBuffer() const;
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

    // for testing
    std::vector<LevelSnapshot> getLevelsSnapshot() const;

    // buffer methods
    void flushBuffer();
    
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
    std::optional<DataPair> getData(long key);

    // protects all flush: only one thread can flush at a time
    std::mutex flush_mutex_;
    std::mutex compaction_mutex_;

    // set up DB directory and history
    void setupDB();
    void loadHistory();
    void updateHistory(const std::vector<std::shared_ptr<SSTable>>& to_remove_l,
                       const std::vector<std::shared_ptr<SSTable>>& to_remove_l_next,
                       const std::vector<std::shared_ptr<SSTable>>& to_add_l_next);
    void updateHistoryAdd(std::shared_ptr<SSTable> new_sstable);
    std::string getLevelPath(int level_num) const;
    std::string getFilePath(int level_num, int file_id) const;
    // delete physical file of an SSTable
    void deleteSSTableFile(const std::shared_ptr<SSTable>& sstable);
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


#endif