#include "lsm_tree.hh"
#include <iostream>
#include <queue>
#include <algorithm> 

/**
 * DataPair methods
 */

DataPair::DataPair(long key, long value, bool deleted) {
    this->key_ = key;
    this->value_ = value;
    this->deleted_ = deleted;
}

bool DataPair::operator<(long other_key) const {
    return key_ < other_key;
}

bool DataPair::operator<(const DataPair& other) const {
    return key_ < other.key_;
}

bool DataPair::operator==(const DataPair& other) const {
    return key_ == other.key_;
}

/**
 * SSTable methods
 */
SSTable::SSTable(const std::vector<DataPair>& data, int level_num) {
                // const std::string& file_name) {
    // this->file_name_ = file_name;
    this->level_num_ = level_num;
    this->table_data_ = data;

    // set min and max key
    if (data.empty()) {
        size_ = 0;
        min_key_ = std::numeric_limits<long>::max();
        max_key_ = std::numeric_limits<long>::min();
    } else {
        // this assumes data MUST BE sorted
        size_ = data.size();
        // data is sorted
        min_key_ = data.front().key_;
        max_key_ = data.back().key_;
    }
}

void SSTable::printSSTable() const {
    for (int i = 0; i < table_data_.size(); i++) {
        std::cout << table_data_[i].key_ << ":" << table_data_[i].value_ << ", ";
    }
}

void SSTableSnapshot::printSSTable() const {
    for (int i = 0; i < table_data.size(); i++) {
        std::cout << table_data[i].key_ << ":" << table_data[i].value_ << ", ";
    }
}

std::optional<DataPair> SSTable::getDataPair(long key) const {
    if (!keyInRange(key)) {
        return std::nullopt;
    }

    for (const auto& pair : table_data_) {
        if (pair.key_ == key) {
            if (pair.deleted_) {
                return std::nullopt;
            } else {
                return pair;
            }
        }
    }
    return std::nullopt;
}

bool SSTable::keyInRange(long key) const {
    return key >= min_key_ && key <= max_key_;
}

bool SSTable::keyInSSTable(long key) const {
    if (!keyInRange(key)) { return false; }

    for (const auto& pair : table_data_) {
        if (pair.key_ == key) {
            return true;
        }
    }
    return false;
}

/**
 * Level methods
 * 
 */

// TODO: capacity trigger based on bytes/entries, not SSTables count. 
Level::Level(int level_num, size_t table_capacity, size_t entries_capacity) {
    this->level_num_ = level_num;
    this->table_capacity_ = table_capacity;
    this->entries_capacity_ = entries_capacity;
    this->cur_table_count_ = 0;
    this->cur_total_entries_ = 0;
}

std::vector<std::shared_ptr<SSTable>> Level::getSSTables() const {
    return sstables_;
}


void Level::addSSTable(std::shared_ptr<SSTable> sstable_ptr) {
    // std::unique_lock<std::shared_mutex> lock(mutex_);
    sstables_.push_back(sstable_ptr);
    cur_total_entries_ += sstable_ptr->size_;
    cur_table_count_++;
}

void Level::removeSSTable(std::shared_ptr<SSTable> sstable_ptr) {
    // std::unique_lock<std::shared_mutex> lock(mutex_);
    auto it = std::find(sstables_.begin(), sstables_.end(), sstable_ptr);
    if (it != sstables_.end()) {
        cur_total_entries_ -= (*it)->size_;
        sstables_.erase(it);
        cur_table_count_--;
    }
}

void Level::removeAllSSTables(const std::vector<std::shared_ptr<SSTable>>& tables_to_remove) {
    // std::unique_lock<std::shared_mutex> lock(mutex_);
    if (tables_to_remove.empty() || sstables_.empty()) {
        return;
    }
    std::vector<std::shared_ptr<SSTable>> tables_to_keep;
    size_t new_total_entries = 0;
    size_t new_table_count = 0;

    tables_to_keep.reserve(sstables_.size());

    for (const auto& current_table : sstables_) {
        bool found_to_remove = false;
        for (const auto& table_to_remove : tables_to_remove) {
            if (current_table == table_to_remove) {
                found_to_remove = true;
                break;
            }
        }
        if (!found_to_remove) {
            tables_to_keep.push_back(current_table);
            new_total_entries += current_table->size_;
            new_table_count++;
        }
    }

    sstables_ = std::move(tables_to_keep);
    cur_total_entries_ = new_total_entries;
    cur_table_count_ = new_table_count;
}

// two conditions to trigger compaction!!
bool Level::needsCompaction() const {
    // std::shared_lock<std::shared_mutex> lock(mutex_);
    return (cur_table_count_ >= table_capacity_) || (cur_total_entries_ >= entries_capacity_);
}

void Level::printLevel() const{
    // std::shared_lock<std::shared_mutex> lock(mutex_);
    // iterate through every sstable
    std::cout << "[Level " << level_num_ << "] ";
    for (int i = 0; i < sstables_.size(); i++) {
        std::cout << "Table " << i << ") ";
        sstables_[i]->printSSTable();
    }
    std::cout << std::endl;
}

void LevelSnapshot::printLevel() const{
    // iterate through every sstable
    std::cout << "[Level " << level_num << "] ";
    for (int i = 0; i < sstables.size(); i++) {
        std::cout << "Table " << i << ") ";
        sstables[i].printSSTable();
    }
}

/**
 * Buffer methods
 * 
 */

Buffer::Buffer(size_t capacity, size_t cur_size) {
    this->capacity_ = capacity;
    this->cur_size_ = cur_size;
    // reserve space for buffer_data_ so it doesn't have to resize
    buffer_data_.reserve(capacity);
}

bool Buffer::isFull() const {
    return cur_size_ >= capacity_;
}

// print buffer for debugging
void Buffer::printBuffer() const {
    std::cout << "Buffer: ";
    for (const auto& data : buffer_data_) {
        std::cout << data.key_ << ":" << data.value_ << ", ";
    }
    std::cout << std::endl;
}

std::shared_ptr<SSTable> Buffer::flushBuffer() {
    if (buffer_data_.empty()) {
        return nullptr;
    }
    // // create new SSTable with buffer_data_
    auto sstable_ptr = std::make_shared<SSTable>(buffer_data_, 0);
    // clear buffer
    buffer_data_.clear();
    cur_size_ = 0;
    return sstable_ptr;
}

// buffer is sorted, so flush to level 1 is much easier
bool Buffer::putData(const DataPair& data) {
    // search through buffer to see if data exists, if so, update it
    // will be more efficient once I refactor to a tree/skip list
    auto it = std::lower_bound(buffer_data_.begin(), buffer_data_.end(), data.key_);
    if (it != buffer_data_.end() && it->key_ == data.key_) {
        it->value_ = data.value_;
        it->deleted_ = data.deleted_;
    } else {
        // insert data in sorted order
        buffer_data_.insert(it, data);
        cur_size_++;
    }
    return true;
}

std::optional<DataPair> Buffer::getData(long key) const {
    // search through buffer to see if data exists, for now
    auto it = std::lower_bound(buffer_data_.begin(), buffer_data_.end(), key);
    if (it != buffer_data_.end() && it->key_ == key) {
        return *it;
    }
    return std::nullopt;

    // need to search the levels next, using bloom filter on each level
}

/**
 * LSMTree methods
 * 
 */
LSMTree::LSMTree(const std::string& db_path, 
                 size_t buffer_capacity, 
                 size_t base_level_capacity, 
                 size_t total_levels,
                 size_t level_size_ratio) {
    this->db_path_ = db_path;

    this->buffer_capacity_ = buffer_capacity;
    this->base_level_table_capacity_ = base_level_capacity;
    this->total_levels_ = total_levels;
    this->level_size_ratio_ = level_size_ratio;

    this->buffer_ = std::make_unique<Buffer>(buffer_capacity);

    // create levels, each which bigger capacity
    levels_.reserve(total_levels);
    size_t cur_level_capacity = base_level_capacity;
    for (size_t i = 0; i < total_levels; i++) {
        levels_.push_back(std::make_unique<Level>(i, cur_level_capacity, MAX_ENTRIES_PER_LEVEL));
        cur_level_capacity *= LEVEL_SIZE_RATIO;
    }
}

// can't print otherwise with the mutexes and unique_ptrs
std::vector<LevelSnapshot> LSMTree::getLevelsSnapshot() const {
    std::vector<LevelSnapshot> snapshot;
    snapshot.reserve(levels_.size());

    for (const auto& level_ptr : levels_) {
        if (!level_ptr) continue;

        LevelSnapshot level_snap;
        // std::shared_lock<std::shared_mutex> lock(level_ptr->mutex_);

        // basic level info
        level_snap.level_num = level_ptr->level_num_;
        level_snap.table_capacity = level_ptr->table_capacity_;
        level_snap.entries_capacity = level_ptr->entries_capacity_;
        level_snap.current_table_count = level_ptr->cur_table_count_;
        level_snap.current_total_entries = level_ptr->cur_total_entries_;

        level_snap.sstables.reserve(level_ptr->sstables_.size());
        for (const auto& sstable_ptr : level_ptr->sstables_) {
            if (sstable_ptr) {
                SSTableSnapshot table_snap;
                table_snap.level_num = sstable_ptr->level_num_;
                table_snap.min_key = sstable_ptr->min_key_;
                table_snap.max_key = sstable_ptr->max_key_;
                table_snap.size = sstable_ptr->size_;
                // table_snap.file_name = sstable_ptr->file_name_;
                table_snap.table_data = sstable_ptr->table_data_;
                level_snap.sstables.push_back(table_snap);
            }
        }

        // lock.unlock();

        snapshot.push_back(std::move(level_snap));
    }

    return snapshot;
}

void LSMTree::flushBuffer() {
    // lock mutex
    // std::lock_guard<std::mutex> lock(flush_mutex_);
    // flush buffer to level 0
    std::shared_ptr<SSTable> sstable_ptr = buffer_->flushBuffer();
    if (sstable_ptr == nullptr) {
        return;
    } 
    // check compaction before adding new sstable to l0
    bool compacted = checkCompaction(0);

    // flush L0 buffer to L1
    levels_[0]->addSSTable(sstable_ptr);
}

bool LSMTree::putData(const DataPair& data) {
    // if level is full, flush. put data in buffer either way
    if (buffer_->isFull()) {
        flushBuffer();
    }
    bool rt = buffer_->putData(data);
    return rt;
}

std::optional<DataPair> LSMTree::getData(long key) const {
    // search buffer first
    std::optional<DataPair> data_pair = buffer_->getData(key);
    if (data_pair.has_value()) {
        if (data_pair.value().deleted_) {
            return std::nullopt;
        } else {
            return data_pair;
        }
    }
    // search levels next: 
    // TODO: add bloom filter each level
    for (size_t level_i = 0; level_i < levels_.size(); level_i++) {
        const auto& cur_level = levels_[level_i];
        // skip level if bloom filter says key not in level

        // lock read
        const auto& cur_sstables = cur_level->getSSTables();
        for (const auto& cur_sstable : cur_sstables) {
            // check range: if not in range, continue
            if (!cur_sstable->keyInRange(key)) {
                continue;
            }
            // check if key in sstable
            std::optional<DataPair> data_search_res = cur_sstable->getDataPair(key);
            if (data_search_res.has_value()) {
                return data_search_res;
            } else {
                if (cur_sstable->keyInSSTable(key)) {
                    // tombstone
                    return std::nullopt;
                }
            }
        }
    }
    return std::nullopt;
}


// compaction logic: return compacted or not
bool LSMTree::checkCompaction(size_t level_index) {
    if (level_index >= levels_.size()) {
        return false;
    }
    auto& cur_level = levels_[level_index];
    // need to flush first before adding new sstable if current level is full

    bool needs_compaction = cur_level->needsCompaction();
    if (needs_compaction) {
        std::cout << "[LSMTree] Level " << level_index << " needs compaction..." << std::endl;
        compactLevel(level_index);
    }
    return true;
}


void LSMTree::compactLevel(size_t level_index) {
    // TODO: coarse grained lock for now
    // std::lock_guard<std::mutex> compaction_lock(compaction_mutex_);
    {
        // std::shared_lock<std::shared_mutex> lock(levels_[level_index]->mutex_);
        if (!levels_[level_index]->needsCompaction()) {
            std::cout << "[LSMTree] Race condition: compaction for Level " << level_index << " is not needed?" << std::endl;
            return;
        }
    }

    size_t next_level_index = level_index + 1;
    if (next_level_index >= levels_.size()) {
        std::cout << "[LSMTree] Level " << level_index << " is the last level, no compaction" << std::endl;
        // what do I do in this case?
        // TODO: Implement last level compaction strategy (e.g., merge within level)
        return;
    }

    std::cout << "[LSMTree] starting compaction for level " << level_index << " to level " << next_level_index << std::endl;

    // compaction participants
    std::vector<std::shared_ptr<SSTable>> input_tables_level;
    std::vector<std::shared_ptr<SSTable>> input_tables_level_next; 

    { // Scope for shared locks to read level contents
        // std::shared_lock<std::shared_mutex> l1Lock(levels_[level_index]->mutex_);
        // std::shared_lock<std::shared_mutex> l2Lock(levels_[next_level_index]->mutex_);

        // Get all tables from the destination level *once* for overlap checks
        // Use getSSTablesRef assuming caller (this function) manages lock lifetime
        const auto& all_next_level_tables = levels_[next_level_index]->getSSTables();

        if (level_index == 0) {
            input_tables_level = levels_[level_index]->getSSTables();

            // lookup same range tables in next level
            for (const auto& table_L0 : input_tables_level) {
                for (const auto& table_L1 : all_next_level_tables) {
                    // overlap found
                    if (table_L0->max_key_ >= table_L1->min_key_ && table_L0->min_key_ <= table_L1->max_key_) {
                        // avoid dups, if multiple L0 tables overlap an L1 table
                        if (std::find(input_tables_level_next.begin(),
                            input_tables_level_next.end(), table_L1) == input_tables_level_next.end()) {
                              input_tables_level_next.push_back(table_L1);
                         }
                    }
                }
            }
        } else {
            const auto& tables_L = levels_[level_index]->getSSTables();
            if (tables_L.empty()) {
                std::cerr << "[LSMTree] compaction triggered on empty level " << level_index << std::endl;
                return;
            }
            // need oldest table first
            input_tables_level.push_back(tables_L.front());
            const auto& all_tables_next = levels_[next_level_index]->getSSTables();

            // overlapping tables for level l+1
            const auto& table_LN_selected = input_tables_level[0];
            for (const auto& table_LNplus1 : all_tables_next) {
                if (table_LN_selected->max_key_ >= table_LNplus1->min_key_ && table_LN_selected->min_key_ <= table_LNplus1->max_key_) {
                    // input_tables_level_next should have started empty
                    input_tables_level_next.push_back(table_LNplus1); // Add ONLY overlapping
                }
            }

        }
    }

    if (input_tables_level.empty()) {
        std::cout << "[LSMTree] empty source level, aborting compaction." << std::endl;
        return;
    }

    std::cout << "[LSMTree] Merging " << input_tables_level.size() << " tables from L" << level_index
              << " and " << input_tables_level_next.size() << " tables from L" << next_level_index << std::endl;

    // TODO: perform merge logic
    std::vector<std::shared_ptr<SSTable>> output_tables = mergeSSTables(input_tables_level,
        input_tables_level_next, next_level_index);
        
    // atomic replace
    {
        // std::unique_lock<std::shared_mutex> lock1(levels_[level_index]->mutex_);
        // std::unique_lock<std::shared_mutex> lock2(levels_[next_level_index]->mutex_);

        // add and remove cuz we merged
        levels_[level_index]->removeAllSSTables(input_tables_level);
        levels_[next_level_index]->removeAllSSTables(input_tables_level_next);

        for (const auto& table : output_tables) {
            levels_[next_level_index]->addSSTable(table);
        }
    }

    std::cout << "[LSMTree] finished compaction, added " << output_tables.size()
              << " new tables to Level " << next_level_index << "." << std::endl;
    levels_[level_index]->printLevel();
    levels_[next_level_index]->printLevel();
    // cascade compaction
    checkCompaction(next_level_index);
}

// merge function for two SSTables
// TODO: merge logic not optimized
std::vector<std::shared_ptr<SSTable>> LSMTree::mergeSSTables(
    const std::vector<std::shared_ptr<SSTable>>& level_l_tables,
    const std::vector<std::shared_ptr<SSTable>>& level_l_plus_1_tables, int output_level_num) {

    std::vector<std::shared_ptr<SSTable>> output_sstables;
    std::vector<DataPair> current_output_data;

    const size_t TARGET_SSTABLE_SIZE = 100;
    // use a minheap to merge, like the lc problem
    std::priority_queue<MergeEntry, std::vector<MergeEntry>, std::greater<MergeEntry>> min_heap;

    std::vector<std::shared_ptr<SSTable>> all_inputs = level_l_tables;
    all_inputs.insert(all_inputs.end(), level_l_plus_1_tables.begin(), level_l_plus_1_tables.end());

    std::vector<size_t> current_indices(all_inputs.size(), 0);

    // Initialize the heap with the first element from each non-empty input table
    for (size_t i = 0; i < all_inputs.size(); ++i) {
        if (!all_inputs[i]->table_data_.empty()) {
            min_heap.push({all_inputs[i]->table_data_[0], i, (size_t) all_inputs[i]->level_num_});
        }
    }

    long last_key = std::numeric_limits<long>::min();
    bool first_entry = true;

    while (!min_heap.empty()) {
        MergeEntry top = min_heap.top();
        min_heap.pop();

        // if key is same as last processed, it's older, skip
        if (!first_entry && top.data.key_ == last_key) {
            current_indices[top.source_table_index]++;
            size_t next_idx = current_indices[top.source_table_index];
            // push next ele from the same source table
            if (next_idx < all_inputs[top.source_table_index]->table_data_.size()) {
                min_heap.push({all_inputs[top.source_table_index]->table_data_[next_idx],
                               top.source_table_index,
                               (size_t) all_inputs[top.source_table_index]->level_num_});
            }
            // skip older key entry
            continue;
        }

        last_key = top.data.key_;
        first_entry = false;

        // tomestones
        bool is_last_level = (output_level_num == (levels_.size() - 1));
        if (!top.data.deleted_ || !is_last_level) {
             current_output_data.push_back(top.data);
        } else {
             std::cout << "[Merge] dropping tombstone for key " << top.data.key_ << " cuz last level" << std::endl;
        }

        //check if current output table is full after adding element
        if (current_output_data.size() >= TARGET_SSTABLE_SIZE) {
            output_sstables.push_back(std::make_shared<SSTable>(current_output_data, output_level_num));
            current_output_data.clear();
        }
        // push next ele from the same source table
        current_indices[top.source_table_index]++;
        size_t next_idx = current_indices[top.source_table_index];
        if (next_idx < all_inputs[top.source_table_index]->table_data_.size()) {
            min_heap.push({all_inputs[top.source_table_index]->table_data_[next_idx],
                           top.source_table_index,
                           (size_t) all_inputs[top.source_table_index]->level_num_});
        }
    }
    // remaining data
    if (!current_output_data.empty()) {
        output_sstables.push_back(std::make_shared<SSTable>(current_output_data, output_level_num));
    }

    return output_sstables;
}
