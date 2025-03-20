#include "lsm_tree.hh"
#include <iostream>

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

std::optional<DataPair> SSTable::getDataPair(long key) const {
    if (!keyInRange(key)) {
        return std::nullopt;
    }

    for (const auto& pair : table_data_) {
        if (pair.key_ == key) {
            return pair;
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

Level::Level(int level_num, size_t capacity, size_t cur_size) {
    this->level_num_ = level_num;
    this->capacity_ = capacity;
    this->cur_size_ = cur_size;
}

const std::vector<std::shared_ptr<SSTable>>& Level::getSSTables() const {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    return sstables_;
}

void Level::addSSTable(std::shared_ptr<SSTable> sstable_ptr) {
    std::unique_lock<std::shared_mutex> lock(mutex_);
    sstables_.push_back(sstable_ptr);
    cur_size_++;
}

void Level::removeSSTable(std::shared_ptr<SSTable> sstable_ptr) {
    std::unique_lock<std::shared_mutex> lock(mutex_);
    // erase the first instance of sstable_ptr
    auto it = std::find(sstables_.begin(), sstables_.end(), sstable_ptr);

    if (it != sstables_.end()) {
        sstables_.erase(it); // Erase the element
        cur_size_--;  // Decrement current size
    }
}

bool Level::needsCompaction() const {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    return cur_size_ >= capacity_;
}

void Level::printLevel() const{
    std::shared_lock<std::shared_mutex> lock(mutex_);
    std::cout << "Level " << level_num_ << ": " << std::endl;
    for (size_t i = 0; i < sstables_.size(); i++){
        std::cout << "SSTable " << i << ": min key " << sstables_[i]->min_key_
        << ", max key " << sstables_[i]->max_key_ << ", size: " << sstables_[i]->size_ << std::endl;
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

std::shared_ptr<SSTable> Buffer::flushBuffer() {
    if (buffer_data_.empty()) {
        return nullptr;
    }
    // // create new SSTable with buffer_data_
    auto sstable_ptr = std::make_shared<SSTable>(buffer_data_, 1);
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
        levels_.push_back(std::make_unique<Level>(i, cur_level_capacity));
        cur_level_capacity *= LEVEL_SIZE_RATIO;
    }
}

void LSMTree::flushBuffer() {
    // lock mutex
    std::lock_guard<std::mutex> lock(flush_mutex_);
    // flush buffer to level 0
    std::shared_ptr<SSTable> sstable_ptr = buffer_->flushBuffer();
    if (sstable_ptr == nullptr) {
        return;
    }
    // TODO: forget cascading compaction for now
    // cascading compaction: until the max level
    // if level 0 is full, flush to level 1, etc.
    int cur_level = 0;
    // while (cur_level < total_levels_ - 1 &&
    //     levels_[cur_level]->needsCompaction()) {
    //     // flush to next level
    //     std::shared_ptr<SSTable> sstable_ptr = levels_[cur_level]->getSSTables()[0];
    //     levels_[cur_level]->removeSSTable(sstable_ptr);
    //     levels_[cur_level + 1]->addSSTable(sstable_ptr);
    //     cur_level++;
    // }
    if (levels_[0]->needsCompaction()) {
    }
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

    // search levels next
    // need bloom filter implementation
    // for (const auto& level : levels_) {
    //     for (const auto& cur_sstable : level->getSSTables()) {
    //         if (cur_sstable->keyInSSTable(key)) {
    //             data_pair = cur_sstable->getDataPair(key);
    //             if (data_pair.has_value()) {
    //                 return data_pair.value();
    //             }
    //         }
    //     }
    // }
    return std::nullopt;
}




