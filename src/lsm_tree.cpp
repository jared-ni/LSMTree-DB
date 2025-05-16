#include "lsm_tree.hh"
#include <iostream>
#include <queue>
#include <algorithm>
#include <shared_mutex>
#include <chrono>
#include <future>

// helper function to generate SSTable filename
inline std::string generateSSTableFilename(uint64_t file_id) {
    std::stringstream ss;
    ss << std::setw(6) << std::setfill('0') << file_id << ".sst";
    return ss.str();
}

/**
 * DataPair methods
 */

DataPair::DataPair(int key, int value, bool deleted) {
    this->key_ = key;
    this->value_ = value;
    this->deleted_ = deleted;
}

bool DataPair::operator<(int other_key) const {
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
SSTable::SSTable(const std::vector<DataPair>& data, int level_num,
                 const std::string& file_path, const std::string& bf_file_path) :
    bloom_filter_(data.size())
{
    this->table_data_ = data;
    this->level_num_ = level_num;
    this->file_path_ = file_path;
    this->bf_file_path_ = bf_file_path;
    this->data_loaded_ = true;
    // add bloom filter
    // this->bloom_filter_ = BloomFilter(data.size());

    // set min and max key
    if (data.empty()) {
        size_ = 0;
        min_key_ = std::numeric_limits<int>::max();
        max_key_ = std::numeric_limits<int>::min();
    } else {
        // this assumes data MUST BE sorted
        size_ = data.size();
        // data is sorted
        min_key_ = data.front().key_;
        max_key_ = data.back().key_;
        
        // TODO: bloom filter
        bloom_filter_ = BloomFilter(size_);
        for (const auto& dataPair : data) {
            bloom_filter_.add(dataPair.key_);
        }

        // TODO: create fence pointers for binary search
        this->fence_pointers_.clear();
        if (this->size_ > 0) {
            // add fence pointers
            for (size_t i = 0; i < this->size_; i += fence_pointer_block_size_) {
                fence_ptr fp;
                fp.min_key = this->table_data_[i].key_;
                fp.data_offset = i;

                // block size calculation, since it might be the last one
                size_t remain_items = this->size_ - i;
                fp.block_size_actual_ = std::min(remain_items, fence_pointer_block_size_);
                this->fence_pointers_.push_back(fp);
            }
        }
    }

    // writeToDisk
    if (!writeToDisk()) {
        throw std::runtime_error("Failed to persist SSTable to disk");
    }
}

// eager loading bloom filter, but lazy load table data
SSTable::SSTable(int level_num, const std::string& file_path, 
                 const std::string& bf_file_path)
    : bloom_filter_(0)
{
    this->level_num_ = level_num;
    this->file_path_ = file_path;
    this->bf_file_path_ = bf_file_path;
    this->size_ = 0;
    this->data_loaded_ = false;
    this->min_key_ = std::numeric_limits<int>::max();
    this->max_key_ = std::numeric_limits<int>::min();

    // TODO: Bloom filter: setting and loading eagerly

    std::ifstream bf_infile(bf_file_path_, std::ios::binary);

    if (bf_infile) {
        size_t num_bits = 0;
        size_t num_hashes = 0;

        bf_infile.read(reinterpret_cast<char*>(&num_bits), sizeof(num_bits));
        bf_infile.read(reinterpret_cast<char*>(&num_hashes), sizeof(num_hashes));

        // check if read was successful
        if (bf_infile.good()) {
            if (num_bits > 0) {
                size_t num_bytes_expected = (num_bits + CHAR_BIT - 1) / CHAR_BIT;
                std::vector<unsigned char> loaded_bits(num_bytes_expected);
                // read into first element in the array
                bf_infile.read(reinterpret_cast<char*>(loaded_bits.data()), num_bytes_expected);

                // check if read was successful
                if (!bf_infile.fail()) {
                    // read all remaining bytes
                    if (bf_infile.gcount() == static_cast<std::streamsize>(num_bytes_expected)) {
                        this->bloom_filter_ = BloomFilter(num_bits, num_hashes, loaded_bits);
                    } else {
                        std::cerr << "[SSTable Placeholder WARN] Failed to read sufficient Bloom filter bits for "
                                  << this->file_path_ << ". Read " << bf_infile.gcount() << " of " << num_bytes_expected 
                                  << " bytes." << std::endl;
                    }
                } else {
                    // Stream is in a failed state
                    std::cerr << "[SSTable] Stream in failed state after attempting to read Bloom filter bits." << std::endl;
                }
            // if num_bits is 0
            } else {
                this->bloom_filter_ = BloomFilter(0, 0, {});
            }
        // if read was not successful
        } else {
            std::cerr << "[SSTable Placeholder WARN] Failed to read Bloom filter header for " << this->file_path_ << std::endl;
            // bloom_filter_ remains in its default empty state
        }
        bf_infile.close();
    } else {
        std::cout << "[SSTable Placeholder INFO] Bloom filter file " << bf_file_path
                  << " not found. Will be reconstructed if/when main data is loaded." << std::endl;
    }
}

// persistence on SSTable
bool SSTable::writeToDisk() const {
    // parent directory must exist
    std::ofstream outfile(file_path_);

    if (!outfile) {
        std::cerr << "[SSTable] error opening write file " << file_path_ << std::endl;
        return false;
    }

    // TODO: refactor to use binary write
    // key:value:tombstone per line
    for (const auto& pair : table_data_) {
        outfile << pair.key_ << ":" << pair.value_ << ":" << (pair.deleted_ ? 1 : 0) << "\n";
    }
    outfile.close();

    bool sst_write_success = !outfile.fail();
    if (!sst_write_success) {
        std::cerr << "[SSTable] error writing to file " << file_path_ << std::endl;
        return false;
    }

    // TODO: write bloom filter to a .bf file in binary
    std::ofstream bf_outfile(bf_file_path_, std::ios::binary);
    if (!bf_outfile) {
        std::cerr << "[SSTable] error opening bloom filter file " << bf_file_path_ << std::endl;
        return false;
    }
    // write the number of bits and hashes
    size_t num_bits = bloom_filter_.num_bits_;
    size_t num_hashes = bloom_filter_.num_hashes_;

    bf_outfile.write(reinterpret_cast<const char*>(&num_bits), sizeof(num_bits));
    bf_outfile.write(reinterpret_cast<const char*>(&num_hashes), sizeof(num_hashes));

    if (num_bits > 0) {
        const std::vector<unsigned char>& bits = bloom_filter_.bits_;
        if (!bits.empty()) {
            bf_outfile.write(reinterpret_cast<const char*>(bits.data()), bits.size());
        }
    }
    bf_outfile.close();
    bool bf_write_success = !bf_outfile.fail();
    if (!bf_write_success) {
        std::cerr << "[SSTable] error writing bloom filter to file " << bf_file_path_ << std::endl;
        return false;
    }

    return !outfile.fail() && !bf_outfile.fail();
    // return true;
}

//persistence
bool SSTable::loadFromDisk() {
    if (data_loaded_) {
        return true;
    }
    // std::cout << "[SSTable] lazy loading from: " << file_path_ << std::endl;
    std::ifstream infile(file_path_);
    if (!infile) {
        std::cerr << "[SSTable ERROR] Could not open file for reading: " 
                  << file_path_ << " (Error: " << strerror(errno) << ")" << std::endl; // Include system error
        return false;
    }
    // clean slate
    table_data_.clear();
    std::string line;
    int key, value;
    int deleted_int;
    char colon1, colon2;
    int line_num = 0;

    while (std::getline(infile, line)) {
        line_num++;
        // Skip empty lines silently
        if (line.empty()) {
            continue;
        }

        std::stringstream ss(line);
        // parse the line in format key:value:deleted
        if (ss >> key >> colon1 >> value >> colon2 >> deleted_int && colon1 == ':' && colon2 == ':') {
             char remaining_char;
             if (ss >> remaining_char) {
                std::cerr << "[SSTable ERROR] Parsing error: Trailing characters found on line " << line_num << " in " << file_path_ << ": '" << line << "'" << std::endl;
                table_data_.clear();
                infile.close();
                return false;
             }
            // Successfully parsed
            table_data_.emplace_back(key, value, deleted_int == 1);
        } else {
            std::cerr << "[SSTable ERROR] Parsing error on line " << line_num << " in " << file_path_ << ": '" << line << "'" << std::endl;
            table_data_.clear();
            infile.close();
            return false;
        }
    }

    // Check stream state AFTER the loop for non-parsing IO errors
    if (infile.bad()) {
         std::cerr << "[SSTable ERROR] File stream badbit set after reading: " << file_path_ << std::endl;
         infile.close();
         table_data_.clear(); // Data is likely corrupted
         return false;
    }
    infile.close();
    // metadata update after loading
    if (table_data_.empty()) {
        // std::cout << "[SSTable] warning: Loaded empty table data from " << file_path_ << std::endl;
        size_ = 0;
        min_key_ = std::numeric_limits<int>::max();
        max_key_ = std::numeric_limits<int>::min();
    } else {
        size_ = table_data_.size();
        min_key_ = table_data_.front().key_;
        max_key_ = table_data_.back().key_;
        // std::cout << "[SSTable] successfully loaded " << size_ << " entries from " << file_path_ << std::endl;
    }
    data_loaded_ = true;

    // TODO: attempt loading bloom filter
    // placeholder constructor might have already loaded it
    // if num_bits_ is 0 but we've loaded the data, 
    if (this->bloom_filter_.num_bits_ == 0 && !this->table_data_.empty()) {
        std::cout << "[SSTable INFO] Reconstructing Bloom filter for " << this->file_path_
                  << " because persisted .bf was missing and main data is now loaded." << std::endl;

        BloomFilter new_bf(this->table_data_.size());
        for (const auto& dataPair : this->table_data_) {
            new_bf.add(dataPair.key_);
        }
        this->bloom_filter_ = new_bf;
    }

    // TODO: build fence pointers using loaded data
    this->fence_pointers_.clear();
    if (this->size_ > 0) {
        // add fence pointers
        for (size_t i = 0; i < this->size_; i += fence_pointer_block_size_) {
            fence_ptr fp;
            fp.min_key = this->table_data_[i].key_;
            fp.data_offset = i;

            size_t remain_items = this->size_ - i;
            fp.block_size_actual_ = std::min(remain_items, fence_pointer_block_size_);
            this->fence_pointers_.push_back(fp);
        }
    }

    return true;
}



void SSTable::printSSTable() const {
    std::lock_guard<std::mutex> lock(sstable_mutex_);

    if (!data_loaded_) {
        std::cout << "(SSTable on disk: " << file_path_ << " size: " << size_
                  << " range: [" << min_key_ << "," << max_key_ << "]) ";
        return;
    }
    for (int i = 0; i < static_cast<int>(table_data_.size()); i++) {
        std::cout << table_data_[i].key_ << ":" << table_data_[i].value_ << ", ";
    }
}

void SSTableSnapshot::printSSTable() const {
    for (int i = 0; i < static_cast<int>(table_data.size()); i++) {
        std::cout << table_data[i].key_ << ":" << table_data[i].value_ << ", ";
    }
}

// return [start_index, end_index_exclusive) in table_data
// returns nullopt if key outside fence pointer ranges
// min/max already checked in SSTable before calling this function
std::optional<std::pair<size_t, size_t>> SSTable::getFenceRange(int key) const {
    if (this->size_ == 0) {
        return std::nullopt;
    }
    if (this->fence_pointers_.empty()) {
        std::cout << "[SSTable] no fence pointers, returning full range" << std::endl;
        return {{0, this->size_}};
    }
    if (key < fence_pointers_.front().min_key) {
        return std::nullopt;
    }

    // binary search on fence pointers to find the block
    // return first time key < fp.min_key is false, 
    auto it = std::upper_bound(fence_pointers_.begin(), fence_pointers_.end(), key,
        [](int key, const fence_ptr& fp) {
            return key < fp.min_key;
        });
    
    // now it will be the first fence pointer that is greater than key,
    // or fence_pointers.end() if key >= all min_keys
    // assume this function is only called when key is in SSTable's range
    auto prev_fence_ptr = std::prev(it);

    size_t block_start_offset = prev_fence_ptr->data_offset;
    size_t block_end_offset = block_start_offset + prev_fence_ptr->block_size_actual_;

    // check if the key is in the range of this block
    return {{block_start_offset, block_end_offset}};
}

// assume the data must be within the current SSTable range, having checked bloom filter
std::optional<DataPair> SSTable::getDataPair(int key) {
    // persistence check: if data not loaded, load from disk
    if (!data_loaded_) {
        // TODO: lock before checking data and loading
        std::lock_guard<std::mutex> lock(this->sstable_mutex_);
        // load from disk
        if (!loadFromDisk()) {
            std::cerr << "[SSTable] failed to load SSTable from disk: " << file_path_ << std::endl;
            return std::nullopt;
        }
    }
    // check if data is empty
    if (table_data_.empty()) {
        return std::nullopt;
    }

    // use fence pointers to find the range of the key
    size_t start_index = 0;
    size_t end_index_exclusive = table_data_.size();

    std::optional<std::pair<size_t, size_t>> block_range = getFenceRange(key);
    if (block_range.has_value()) {
        start_index = block_range.value().first;
        end_index_exclusive = block_range.value().second;
        // quick sanity check
        if (start_index >= end_index_exclusive || 
            end_index_exclusive > this->size_) {
            std::cerr << "[SSTable] invalid range for key " << key 
                      << " in SSTable " << file_path_ << std::endl;
            start_index = 0;
            end_index_exclusive = this->size_;
        }
    }

    // search within the fence pointer block range
    auto block_begin_it = table_data_.begin() + start_index;
    auto block_end_it = table_data_.begin() + end_index_exclusive;

    // TODO: replace with binary search
    auto it = std::lower_bound(block_begin_it, block_end_it, key,
                                [](const DataPair& dataPair, int key) {
                                    return dataPair.key_ < key;
                                });
    // auto it = std::lower_bound(block_begin_it, block_end_it, key);
    if (it != block_end_it && it->key_ == key) {
        // always return, even if tombstone/deleted, so we can check in the return
        return *it;
    }
    return std::nullopt;
}

bool SSTable::keyInRange(int key) const {
    return key >= min_key_ && key <= max_key_;
}

/**
 * Level methods
 * 
 */

// TODO: capacity trigger based on bytes/entries, not SSTables count. 
Level::Level(int level_num, size_t table_capacity) {
            //  size_t entries_capacity) {
    this->level_num_ = level_num;
    this->table_capacity_ = table_capacity;
    // this->entries_capacity_ = entries_capacity;
    this->cur_table_count_ = 0;
    this->cur_total_entries_ = 0;
}

// READ: shared locked
std::vector<std::shared_ptr<SSTable>> Level::getSSTables() const {
    // lock the level mutex when getting it
    // std::lock_guard<std::mutex> lock(level_mutex_);
    std::shared_lock lock(this->level_mutex_);
    return sstables_;
}

// WRITE: uniquely LOCKED when add sstable
void Level::addSSTable(std::shared_ptr<SSTable> sstable_ptr) {
    // std::lock_guard<std::mutex> lock(level_mutex_);
    std::unique_lock lock(this->level_mutex_);

    sstables_.push_back(sstable_ptr);
    cur_total_entries_ += sstable_ptr->size_;
    cur_table_count_++;
}

// write: uniquely LOCKED
void Level::removeSSTable(std::shared_ptr<SSTable> sstable_ptr) {
    // std::lock_guard<std::mutex> lock(level_mutex_);
    std::unique_lock lock(level_mutex_);

    auto it = std::find(sstables_.begin(), sstables_.end(), sstable_ptr);
    if (it != sstables_.end()) {
        cur_total_entries_ -= (*it)->size_;
        sstables_.erase(it);
        cur_table_count_--;
    }
}

// write: uniqued LOCKED
void Level::removeAllSSTables(const std::vector<std::shared_ptr<SSTable>>& tables_to_remove) {
    // std::lock_guard<std::mutex> lock(level_mutex_);
    std::unique_lock lock(level_mutex_);

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
// read: shared lock
bool Level::needsCompaction() const {
    // std::lock_guard<std::mutex> lock(level_mutex_);
    std::shared_lock lock(level_mutex_);
    return (cur_table_count_ >= table_capacity_);
    // || (cur_total_entries_ >= entries_capacity_);
}

void Level::printLevel() const{
    // iterate through every sstable
    std::cout << "[Level " << level_num_ << "] ";
    for (int i = 0; i < static_cast<int>(sstables_.size()); i++) {
        std::cout << "Table " << i << ") ";
        sstables_[i]->printSSTable();
    }
    std::cout << std::endl;
}

void LevelSnapshot::printLevel() const{
    // iterate through every sstable
    std::cout << "[Level " << level_num << "] ";
    for (int i = 0; i < static_cast<int>(sstables.size()); i++) {
        std::cout << "Table " << i << ") ";
        sstables[i].printSSTable();
    }
}

/**
 * Buffer methods
 * 
 */

Buffer::Buffer(size_t capacity) {
    this->capacity_ = capacity;
    // reserve space for buffer_data_ so it doesn't have to resize
    // buffer_data_.reserve(capacity);
}

bool Buffer::isFull() const {
    // lock the buffer mutex
    // std::lock_guard<std::mutex> lock(this->buffer_mutex_);
    std::shared_lock lock(this->buffer_mutex_);
    // return cur_size_ >= capacity_;
    return buffer_data_.size() >= capacity_;
}

// print buffer for debugging
void Buffer::printBuffer() const {
    std::shared_lock lock(this->buffer_mutex_);
    std::cout << "Buffer: ";
    for (const auto& pair_entry : buffer_data_) {
        const DataPair& data = pair_entry.second;
        std::cout << data.key_ << ":" << data.value_ << ", ";
    }
    std::cout << std::endl;
}

// std::shared_ptr<SSTable> Buffer::flushBuffer() {
//     if (buffer_data_.empty()) {
//         return nullptr;
//     }
//     // // create new SSTable with buffer_data_
//     auto sstable_ptr = std::make_shared<SSTable>(buffer_data_, 0);
//     // clear buffer
//     buffer_data_.clear();
//     cur_size_ = 0;
//     return sstable_ptr;
// }

// buffer is sorted, so flush to level 1 is much easier
// TODO: refactor to use a tree/skip list, or add binary search
bool Buffer::putData(const DataPair& data) {
    // first load the buffer
    // exclusive lock on the write to buffer
    // std::lock_guard<std::mutex> lock(this->buffer_mutex_);
    std::unique_lock lock(this->buffer_mutex_);

    // search through buffer to see if data exists, if so, update it
    // will be more efficient once I refactor to a tree/skip list
    // auto it = std::lower_bound(buffer_data_.begin(), buffer_data_.end(), data.key_);
    // if (it != buffer_data_.end() && it->key_ == data.key_) {
    //     it->value_ = data.value_;
    //     it->deleted_ = data.deleted_;
    // } else {
    //     // insert data in sorted order
    //     buffer_data_.insert(it, data);
    //     cur_size_++;
    // }
    buffer_data_.insert_or_assign(data.key_, data);
    return true;
}

// get data from buffer, shared mutex
std::optional<DataPair> Buffer::getData(int key) const {
    // lock the get operation in the buffer
    // std::shared_lock<std::shared_mutex> lock(this->buffer_mutex_);
    std::shared_lock lock(this->buffer_mutex_);

    // search through buffer to see if data exists, for now
    // auto it = std::lower_bound(buffer_data_.begin(), buffer_data_.end(), key);
    // if (it != buffer_data_.end() && it->key_ == key) {
    //     return *it;
    // }
    auto it = buffer_data_.find(key);
    if (it != buffer_data_.end()) {
        return it->second;
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
    // path for history of SSTables
    this->history_path_ = db_path + "/history";

    this->buffer_ = std::make_unique<Buffer>(buffer_capacity);

    // create levels, each which bigger capacity
    levels_.reserve(total_levels);
    size_t cur_level_capacity = base_level_capacity;
    for (size_t i = 0; i < total_levels; i++) {
        // levels_.push_back(std::make_unique<Level>(i, cur_level_capacity, MAX_ENTRIES_PER_LEVEL));
        levels_.push_back(std::make_unique<Level>(i, cur_level_capacity));
        std::cout << "[LSMTree] Created Level " << i 
                  << " with table capacity: " << cur_level_capacity 
                  << " (buffer capacity: " << buffer_capacity_ << ")" << std::endl;
        cur_level_capacity *= level_size_ratio;
    }

    // configure file system
    setupDB();

    // start background threads
    this->flusher_thread_ = std::thread(&LSMTree::flushThreadLoop, this);
    this->compactor_thread_ = std::thread(&LSMTree::compactThreadLoop, this);

    std::cout << "[LSMTree] Started 1 flusher and " 
              << " 1 compactor thread." << std::endl;
}

// destructor for thread consistency
LSMTree::~LSMTree() {
    shutdown();
    std::cout << "[LSMTree] Shutdown complete." << std::endl;
}

// shutdown function for a background thread
void LSMTree::shutdown() {
    // set to three and return val before set
    if (shutdown_requested_.exchange(true)) {
        return;
    }
    std::cout << "[LSMTree] Shutdown requested. Waiting for threads to finish..." << std::endl;

    // wake up all threads to check shutdown flag
    flush_request_cv_.notify_all();
    compaction_task_cv_.notify_all();

    // TODO: join threads
    if (flusher_thread_.joinable()) {
        flusher_thread_.join();
        std::cout << "[LSMTree] Flusher thread joined." << std::endl;
    }
    if (compactor_thread_.joinable()) {
        compactor_thread_.join();
        std::cout << "[LSMTree] Compactor thread joined." << std::endl;
    }
}


void LSMTree::setupDB() {
    std::error_code ec;
    // makes sure path looks good
    if (!std::filesystem::exists(db_path_) || 
        !std::filesystem::is_directory(db_path_)) {
        if (!std::filesystem::create_directories(db_path_, ec)) {
            std::cerr << "Failed creating db dir " << db_path_ << ": " << ec.message() << std::endl;
            throw std::runtime_error("failed to create DB directory");
        }
    }

    // configure each level
    int max_loaded_file_id = 0;
    for (size_t i = 0; i < total_levels_; ++i) {
        std::string level_path_str = getLevelPath(i);
        std::filesystem::path level_path(level_path_str);
        std::string bf_dir_path_str = level_path_str + "/bloom_filters";
        std::filesystem::path bf_dir_path(bf_dir_path_str);

        // directories exist
        if (!std::filesystem::exists(level_path)) {
            if (!std::filesystem::create_directory(level_path, ec)) {
                std::cerr << "failed to create level directory " << level_path_str << ": " << ec.message() << std::endl;
                throw std::runtime_error("failed to create level directory");
            }
        } else if (!std::filesystem::is_directory(level_path)) {
             std::cerr << "Level path " << level_path_str << " exists but is not a directory." << std::endl;
            throw std::runtime_error("Level path is not a directory");
        }

        // bloom filter subdirectory exists
        if (!std::filesystem::exists(bf_dir_path)) {
            if (!std::filesystem::create_directory(bf_dir_path, ec)) {
                std::cerr << "failed to create bloom filter directory " << bf_dir_path_str << ": " << ec.message() << std::endl;
                throw std::runtime_error("failed to create bloom filter directory");
            }
        } else if (!std::filesystem::is_directory(bf_dir_path)) {
            std::cerr << "Bloom filter path " << bf_dir_path_str << " exists but is not a directory." << std::endl;
            throw std::runtime_error("Bloom filter path is not a directory");
        }
        // need to load the sstables for the level
        std::vector<std::pair<uint64_t, std::shared_ptr<SSTable>>> loaded_sstables_for_level;

        // Load SSTables for this level
        if (std::filesystem::exists(level_path) && std::filesystem::is_directory(level_path)) {
            
            for (const auto& entry : std::filesystem::directory_iterator(level_path)) {
                if (entry.is_regular_file() && entry.path().extension() == ".sst") {
                    std::string sst_filename = entry.path().filename().string();
                    std::string sst_file_path_str = entry.path().string();
                    
                    try {
                        // 000000.sst, 000001.sst, 000000 is first
                        size_t dot_pos = sst_filename.find('.');
                        if (dot_pos == std::string::npos) {
                             std::cerr << "[LSMTree::setupDB] Invalid SSTable filename (no extension): " << sst_filename << ". Skipping." << std::endl;
                             continue;
                        }
                        // get the file id
                        std::string id_str = sst_filename.substr(0, dot_pos);
                        // Discards any whitespace characters, then cast to an integer
                        uint64_t file_id = std::stoull(id_str);
                        
                        if (file_id > max_loaded_file_id) {
                            max_loaded_file_id = file_id;
                        }

                        std::string bf_file_path_str = getBloomFilterPath(i, file_id);

                        // SSTable placeholder that loads the bloom filter
                        auto sstable_ptr = std::make_shared<SSTable>(i, sst_file_path_str, bf_file_path_str);
                        
                        // load the data and metadata: min/max keys, size
                        // TODO: load just the bloom filter, min/max keys, fence pointers
                        if (!sstable_ptr->data_loaded_) {
                            // Attempt to load the data from disk
                            bool data_loaded = sstable_ptr->loadFromDisk();
                            if (!data_loaded) { 
                                std::cerr << "[LSMTree::setupDB] Failed to load data for SSTable " 
                                          << sst_file_path_str << ". Skipping." << std::endl;
                                continue; 
                            }
                        }
                        loaded_sstables_for_level.push_back({file_id, sstable_ptr});

                    } catch (const std::invalid_argument& ia) {
                        std::cerr << "[LSMTree::setupDB] Invalid argument for SSTable filename: " 
                                  << sst_filename << ". Skipping. Error: " << ia.what() << std::endl;
                    } catch (const std::out_of_range& oor) {
                        std::cerr << "[LSMTree::setupDB] Filename number out of range for SSTable: " 
                                  << sst_filename << ". Skipping. Error: " << oor.what() << std::endl;
                    } catch (const std::exception& e) {
                        std::cerr << "[LSMTree::setupDB] Error processing SSTable " << sst_filename 
                                  << ": " << e.what() << ". Skipping." << std::endl;
                    }
                }
            }
        }
        
        // sort SSTables by ascending file_id before adding to the level
        std::sort(loaded_sstables_for_level.begin(), loaded_sstables_for_level.end(),
                  [](const auto& a, const auto& b) {
                      return a.first < b.first;
                  });
        
        // add sorted sstables to level memory
        for (const auto& pair : loaded_sstables_for_level) {
            levels_[i]->addSSTable(pair.second);
        }
         std::cout << "[LSMTree::setupDB] Level " << i << " loaded with " 
                   << loaded_sstables_for_level.size() << " SSTables." << std::endl;
    }

    // set the next_file_id_ based on the maximum ID found on disk
    if (max_loaded_file_id > 0) {
        next_file_id_.store(max_loaded_file_id + 1);
    } else {
        next_file_id_.store(1);
    }
    std::cout << "[LSMTree::setupDB] Database setup complete. Next file ID will be: " << next_file_id_.load() << std::endl;

    // may add history file? probably not
    if (!std::filesystem::exists(history_path_)) {
        std::ofstream history_file(history_path_); 
        if (!history_file) {
            std::cerr << "[LSMTree::setupDB] Warning: Could not create history file at " << history_path_ << std::endl;
        }
    }
}

std::string LSMTree::getLevelPath(int level_num) const {
    return db_path_ + "/level_" + std::to_string(level_num);
}

std::string LSMTree::getFilePath(int level_num, int file_id) const {
    return getLevelPath(level_num) + "/" + generateSSTableFilename(file_id);
}

std::string LSMTree::getBloomFilterPath(int level_num, int file_id) const {
    return getLevelPath(level_num) + "/bloom_filters/" + generateSSTableFilename(file_id) + ".bf";
}


// TODO: persistence logic of loadHistory()
void LSMTree::loadHistory() {}

// can't print otherwise with the mutexes and unique_ptrs
std::vector<LevelSnapshot> LSMTree::getLevelsSnapshot() const {
    std::vector<LevelSnapshot> snapshot;
    snapshot.reserve(levels_.size());

    for (const auto& level_ptr : levels_) {
        if (!level_ptr) continue;

        LevelSnapshot level_snap;

        // basic level info
        level_snap.level_num = level_ptr->level_num_;
        level_snap.table_capacity = level_ptr->table_capacity_;
        // level_snap.entries_capacity = level_ptr->entries_capacity_;
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
    // need to lock buffer before accessing it
    // flush buffer to level 0
    std::vector<DataPair> data_to_flush;
    bool buffer_was_empty = true;
    // ciritical section: access and clear buffer
    {
        std::unique_lock<std::shared_mutex> buffer_lock(buffer_->buffer_mutex_);
        if (buffer_->buffer_data_.empty()) {
            return;
        }
        // data_to_flush = buffer_->buffer_data_;
        data_to_flush.reserve(buffer_->buffer_data_.size());
        for (const auto& pair : buffer_->buffer_data_) {
            data_to_flush.push_back(pair.second);
        }
        buffer_->buffer_data_.clear();
        buffer_was_empty = false;
    }
    // if buffer is empty, we don't flush
    if (buffer_was_empty) {
        return;
    }
    
    // now for flush operation, lock flush_mutex_
    std::lock_guard<std::mutex> flush_lock(flush_mutex_);

    // generate new level 0 SSTable id and file path
    uint64_t new_file_id = next_file_id_++;
    std::string new_file_path = getFilePath(0, new_file_id);
    std::string bf_file_path = getBloomFilterPath(0, new_file_id);

    std::shared_ptr<SSTable> sstable_ptr = nullptr;
    try {
        // create the SSTable object and write to disk
        sstable_ptr = std::make_shared<SSTable>(data_to_flush, 0, new_file_path, bf_file_path);
    } catch (const std::exception& e) {
        std::cerr << "can't create/write SSTable during flush: " << e.what() << std::endl;
        // If file creation failed revert file id
        next_file_id_--;
        // maybe throw error here? idk
        return;
    }

    // add the new SSTable pointer to level 0's list
    levels_[0]->addSSTable(sstable_ptr);

    // trigger compaction check before adding to Level 0 in memory
    {
        // acquire compaction lock, to ensure it's acquired one time
        std::lock_guard<std::mutex> compaction_lock(compaction_mutex_);
        this->checkCompaction(0);
    }
}

bool LSMTree::putData(const DataPair& data) {
    // if (shutdown_requested_) {
    //     return false;
    // }
    bool rt = buffer_->putData(data);
    // if level is full, flush. put data in buffer either way

    // isfull locks buffer_mutex_
    bool should_flush = false;
    {
        if (buffer_->isFull()) {
            should_flush = true;
        }
    }
    // flushBuffer locks flush_mutex_
    // to parallelize, use a conditional variable that flusher thread waits on
    // single thread: call flushBuffer directly
    if (should_flush) {
        // the flush_mutex now only protects the flush_needed_ variable
        std::lock_guard lock(this->flush_mutex_);
        this->flush_needed_ = true;
        flush_request_cv_.notify_one();
        // this->flushBuffer();
    }

    return rt;
}

std::optional<DataPair> LSMTree::getData(int key) {
    // in case shut down thread
    // if (shutdown_requested_) return std::nullopt;

    // search buffer first
    // getData locks buffer_mutex_
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

        // TODO: skip level if bloom filter says key not in level
        // getSSTables locks level_mutex_
        std::vector<std::shared_ptr<SSTable>> cur_sstables;
        {
            std::shared_lock lock(cur_level->level_mutex_);
            cur_sstables = cur_level->sstables_;
        }
        // newer table priority, so we process the new/last tables first
        std::reverse(cur_sstables.begin(), cur_sstables.end());

        for (const auto& cur_sstable : cur_sstables) {
            // check range: if not in range, continue
            if (!cur_sstable->keyInRange(key)) {
                continue;
            }

            // check bloom filter
            if (!cur_sstable->bloom_filter_.might_contain(key)) {
                continue;
            }

            // use fence pointers in getDataPair, now that we know it's in it
            std::optional<DataPair> data_search_res = cur_sstable->getDataPair(key);
            if (data_search_res.has_value()) {
                // need to check for tombstone
                if (data_search_res.value().deleted_) {
                    return std::nullopt;
                }
                return data_search_res;
            }
        }
    }
    return std::nullopt;
}

// range data API, returns all data in range [low, high)
std::vector<DataPair> LSMTree::rangeData(int low, int high) {
    // if (shutdown_requested_) return std::vector<DataPair>();
    std::vector<DataPair> final_results;
    std::map<int, DataPair> results_map;

    // scan the buffer
    {
        std::shared_lock<std::shared_mutex> lock(buffer_->buffer_mutex_);
        // auto it_low = std::lower_bound(buffer_->buffer_data_.begin(), 
        //                                buffer_->buffer_data_.end(), low);
        // for (auto it = it_low; it != buffer_->buffer_data_.end() && it->key_ < high; ++it) {
        auto it_low = buffer_->buffer_data_.lower_bound(low);
        for (auto it = it_low; it != buffer_->buffer_data_.end() && it->first < high; ++it) {
            // add to results_map
            // only add if doesn't exist already
            // results_map.emplace(it->key_, *it);
            results_map.emplace(it->first, it->second);
        }
    }
    // scan SSTables on disk level by level
    for (size_t level_i = 0; level_i < levels_.size(); ++level_i) {
        std::vector<std::shared_ptr<SSTable>> sstables_to_scan;
        {
            // can't be const, so can't use getSSTables()
            std::shared_lock lock(levels_[level_i]->level_mutex_);
            sstables_to_scan = levels_[level_i]->sstables_;
        }

        // newer table priority, so we process the new/last tables first
        std::reverse(sstables_to_scan.begin(), sstables_to_scan.end());

        for (const auto& sstable_ptr : sstables_to_scan) {
            // skip if not in range
            if (sstable_ptr->max_key_ < low || sstable_ptr->min_key_ > high) {
                continue;
            }

            // TODO: bloom filter check

            std::vector<DataPair> sstable_data;
            bool load_and_successful = false;
            {
                std::lock_guard<std::mutex> lock(sstable_ptr->sstable_mutex_);
                if (!sstable_ptr->data_loaded_) {
                    if (!sstable_ptr->loadFromDisk()) {
                        std::cerr << "[LSMTree] Error loading SSTable data from disk." << std::endl;
                        continue;
                    }
                }
            }
            
            // check again after potential load
            if (sstable_ptr->data_loaded_) {
                sstable_data = sstable_ptr->table_data_;
                load_and_successful = true;
            }
            if (sstable_data.empty() || !load_and_successful) {
                continue;
            }
            // binary search on [first, last), return first element not less than low
            // so we can start fill in there
            auto it_low = std::lower_bound(sstable_data.begin(), 
                                           sstable_data.end(), low);
            for (auto it = it_low; it->key_ < high && it != sstable_data.end(); ++it) {
                // if key not in results_map, add
                // level by level: lower level first
                // only add if doesn't exist in results_map yet
                if (results_map.find(it->key_) == results_map.end()) {
                    results_map.emplace(it->key_, *it);
                }
            }
        }
    }

    // populate final results using results_map, and filter out tombstones
    final_results.reserve(results_map.size());
    for (const auto& pair_entry : results_map) {
        if (!pair_entry.second.deleted_) {
            final_results.push_back(pair_entry.second);
        }
    }
    return final_results;
}

// delete is just putting in the tombstone in the buffer for now
bool LSMTree::deleteData(int key) {
    // mark data in buffer as tombstone, if found
    DataPair tombstone_data(key, 0, true);
    
    return this->putData(tombstone_data);
}

// persistence
// delete the SSTable file and its bloom filter
// can we delete in the background?
void LSMTree::deleteSSTableFile(const std::shared_ptr<SSTable>& sstable) {
    if (!sstable || sstable->file_path_.empty()) {
        return;
    }

    std::error_code ec;
    if (std::filesystem::remove(sstable->file_path_, ec)) {
        // std::cout << "[LSMTree] delete SSTable file: " << sstable->file_path_ << std::endl;
    } else {
        std::cerr << "Warning: fail to delete SSTable file " << sstable->file_path_ << ": " << ec.message() << std::endl;
    }
    // delete the bloom filter file
    if (std::filesystem::remove(sstable->bf_file_path_, ec)) {
        // std::cout << "[LSMTree] delete Bloom filter file: " << sstable->bf_file_path_ << std::endl;
    } else {
        std::cerr << "Warning: fail to delete Bloom filter file " << sstable->bf_file_path_ << ": " << ec.message() << std::endl;
    }
}


// compaction logic: return compacted or not
bool LSMTree::checkCompaction(size_t level_index) {
    if (level_index >= levels_.size()) {
        return false;
    }
    // need to flush first before adding new sstable if current level is full
    bool needs_compaction = false;
    {
        // needsCompaction locks level_mutex_
        needs_compaction = levels_[level_index]->needsCompaction();
    }

    if (needs_compaction) {
        // std::cout << "[LSMTree] Level " << level_index << " needs compaction..." << std::endl;
        // compactLevel locks compaction_mutex_
        compactLevel(level_index);
        return true;
    }
    return false;
}


// basic tiering
// this is the first call to compactLevel, which has the global compaction_lock
// subsequent recursive calls don't have the lock
void LSMTree::compactLevel(size_t level_index) {
    // global compaction lock go before calling compaction check
    if (level_index >= levels_.size()) {
        std::cerr << "[LSMTree] Invalid level index for compaction: " << level_index << std::endl;
        return;
    }

    // re-check needsCompaction while holding the global lock to avoid race
    bool needs_compaction = false;
    {
        needs_compaction = levels_[level_index]->needsCompaction();
    }
    if (!needs_compaction) {
        return;
    }

    size_t next_level_index = level_index + 1;
    if (next_level_index >= levels_.size()) {
        // std::cout << "[LSMTree] Level " << level_index << " is the last level, no compaction" << std::endl;
        // tiered compaction stops at the last level
        return;
    }
    if (!levels_[next_level_index]) {
        std::cerr << "[LSMTree Compaction ERROR] Next level " << next_level_index << " does not exist." << std::endl;
        return;
    }

    // tiered compaction: merge all tables in the current level into the next level
    // compaction participants: input tables level next is always empty for tiering
    std::vector<std::shared_ptr<SSTable>> input_tables_level;
    std::vector<std::shared_ptr<SSTable>> input_tables_level_next;

    // Select ALL tables from the current level (tier) for compaction
    // getSSTables locks level_mutex_
    input_tables_level = levels_[level_index]->getSSTables();
    if (input_tables_level.empty()) {
        std::cerr << "[LSMTree Compaction ERROR] No tables in current level " << level_index << " to compact." << std::endl;
        return;
    }

    // Perform merge logic - mergeSSTables now takes an empty input_tables_level_next
    std::vector<std::shared_ptr<SSTable>> output_tables;
    try {
        // mergeSSTables doesn't lock levels_ since it copies the data when merging them
        output_tables = mergeSSTables(input_tables_level,
                                      input_tables_level_next,
                                      next_level_index);
    } catch (const std::exception& e) {
        std::cerr << "Error during tiered SSTable merge: " << e.what() << std::endl;
        return;
    }

    // atomic replace in memory: protected by compaction_mutex_
    // removeAllSSTables locks level_mutex_
    levels_[level_index]->removeAllSSTables(input_tables_level);

    for (const auto& table : output_tables) {
        levels_[next_level_index]->addSSTable(table);
    }

    // TODO: updateHistory

    // delete the SSTable files that were merged from the current level
    for (const auto& table : input_tables_level) {
        deleteSSTableFile(table);
    }

    // after compacted this level, compact the next if needed
    checkCompaction(next_level_index);
}

// merge function for multiple SSTables, returns a list of SSTables limited by size
// TODO: merge logic not optimized
std::vector<std::shared_ptr<SSTable>> LSMTree::mergeSSTables(
    const std::vector<std::shared_ptr<SSTable>>& level_l_tables,
    const std::vector<std::shared_ptr<SSTable>>& level_l_plus_1_tables,
    int output_level_num) {

    std::vector<std::shared_ptr<SSTable>> output_sstables;
    std::vector<DataPair> current_output_data;
    // todo: need to redefine this later
    const size_t TARGET_SSTABLE_SIZE = MAX_TABLE_SIZE; 

    std::priority_queue<MergeEntry, std::vector<MergeEntry>, std::greater<MergeEntry>> min_heap;

    std::vector<std::shared_ptr<SSTable>> all_inputs = level_l_tables;
    all_inputs.insert(all_inputs.end(), level_l_plus_1_tables.begin(), level_l_plus_1_tables.end());

    // load all input data first before merge
    std::vector<std::vector<DataPair>> input_data_vecs(all_inputs.size());
    std::vector<size_t> input_levels(all_inputs.size());

    for(size_t i = 0; i < all_inputs.size(); ++i) {
        std::vector<DataPair> table_data_copy;
        {
            std::lock_guard<std::mutex> lock(all_inputs[i]->sstable_mutex_);
            if (!all_inputs[i]->data_loaded_) {
                if (!all_inputs[i]->loadFromDisk()) {
                    std::cerr << "Error loading input table " << all_inputs[i]->file_path_ << " for merge." << std::endl;
                    throw std::runtime_error("Failed to load input SSTable for merge");
                }
            }
        }
    
        input_data_vecs[i] = all_inputs[i]->table_data_; 
        input_levels[i] = all_inputs[i]->level_num_;
        // do I unload the data? 
    }

    // make heap with first element from each input vector
    std::vector<size_t> current_indices(all_inputs.size(), 0);
    for (size_t i = 0; i < input_data_vecs.size(); ++i) {
        if (!input_data_vecs[i].empty()) {
            min_heap.push({input_data_vecs[i][0], i, input_levels[i]});
        }
    }

    int last_key = std::numeric_limits<int>::min();
    bool first_entry = true;

    // K-way merge using the heap
    while (!min_heap.empty()) {
        MergeEntry top = min_heap.top();
        min_heap.pop();

        if (!first_entry && top.data.key_ == last_key) {
            current_indices[top.source_table_index]++;
            size_t next_idx = current_indices[top.source_table_index];
             if (next_idx < input_data_vecs[top.source_table_index].size()) {
                 min_heap.push({input_data_vecs[top.source_table_index][next_idx],
                                top.source_table_index,
                                input_levels[top.source_table_index]});
             }
            continue;
        }

        last_key = top.data.key_;
        first_entry = false;

        //tombstoness
        bool is_last_level = (output_level_num == static_cast<int>((levels_.size() - 1)));
        if (!top.data.deleted_ || !is_last_level) {
            current_output_data.push_back(top.data);
        } // else drop the entry if it's a tombstone and we're at the last level

        //check if the current output buffer is full
        if (current_output_data.size() >= TARGET_SSTABLE_SIZE) {
            uint64_t new_file_id = next_file_id_++;
            std::string new_file_path = getFilePath(output_level_num, new_file_id);
            std::string new_bloom_filter_path = getBloomFilterPath(output_level_num, new_file_id);
            auto new_sstable = std::make_shared<SSTable>(current_output_data, output_level_num, new_file_path, new_bloom_filter_path);
            output_sstables.push_back(new_sstable);
            // std::cout << "[Merge] Created output SSTable: " << new_file_path << std::endl;
            current_output_data.clear(); // Reset buffer for the next file
        }
        // push heap
        current_indices[top.source_table_index]++;
        size_t next_idx = current_indices[top.source_table_index];
        if (next_idx < input_data_vecs[top.source_table_index].size()) {
            min_heap.push({input_data_vecs[top.source_table_index][next_idx],
                           top.source_table_index,
                           input_levels[top.source_table_index]});
        }
    }

    if (!current_output_data.empty()) {
        uint64_t new_file_id = next_file_id_++;
        std::string new_file_path = getFilePath(output_level_num, new_file_id);
        std::string new_bloom_filter_path = getBloomFilterPath(output_level_num, new_file_id);
        auto new_sstable = std::make_shared<SSTable>(current_output_data, output_level_num, new_file_path, new_bloom_filter_path);
        output_sstables.push_back(new_sstable);
        // std::cout << "[Merge] created final output SSTable: " << new_file_path << std::endl;
    }

    return output_sstables;
}

// background thread loops for flushing and compaction
// asleep in wait, and only acquires the flush_mutex_ when it is notified/waked
void LSMTree::flushThreadLoop() {
    std::cout << "[Flusher Thread] Started." << std::endl;
    while (true) {
        bool should_flush = false;
        {
            std::unique_lock lock(this->flush_mutex_);
            // wait until notified: then check flush_needed_
            flush_request_cv_.wait(lock, [this] {
                return shutdown_requested_ || flush_needed_;
            });
            // spurious wake-up
            if (flush_needed_) {
                should_flush = true;
                flush_needed_ = false;
            }
        }

        if (should_flush) {
            flushBufferHelper();
            // check if L0 now needs compaction, after the flush
            doCompactionCheck(0);
        }
        if (shutdown_requested_.load()) {
            break;
        }
    }
    std::cout << "[Flusher Thread] Exiting." << std::endl;
}

// compactorLoop
void LSMTree::compactThreadLoop() {
    std::cout << "[Compactor Thread " << std::this_thread::get_id() 
              << "] Started." << std::endl;

    while (true) {
        int level_to_compact = -1;
        {
            std::unique_lock lock(this->compaction_mutex_);
            // get notified if compaction task is available
            compaction_task_cv_.wait(lock, [this]{
                return shutdown_requested_.load() || !compaction_tasks_.empty();
            });

            // spurious wake-up check
            // take the front of the tasks, FIFO style
            if (!compaction_tasks_.empty()) {
                // level_to_compact = compaction_tasks_.front();
                level_to_compact = compaction_tasks_.top();
                compaction_tasks_.pop();
            }
        }

        // we have a level to compact
        if (level_to_compact != -1 && static_cast<unsigned long>(level_to_compact) < levels_.size() - 1) {
            try {
                // compact current level, and check the next one right after
                // includes doCompactionCheck for level_to_compact + 1
                compactLevelHelper(level_to_compact);
            } catch (const std::exception& e) {
                std::cerr << "[Compactor Thread " << std::this_thread::get_id() 
                          << "] Exception during compaction of level "
                          << level_to_compact << ": " << e.what() << std::endl;
            }
        }
        // only shut down if no more compaction tasks
        if (shutdown_requested_.load() && compaction_tasks_.empty()) {
            std::cout << "[Compactor Thread " << std::this_thread::get_id() << "] Exiting." << std::endl;
            break;
        }
    }
}

void LSMTree::flushBufferHelper() {
    std::vector<DataPair> data_to_flush;
    bool buffer_was_empty = true;
    // lock buffer and get data
    {
        std::unique_lock buffer_lock(buffer_->buffer_mutex_);
        if (buffer_->buffer_data_.empty()) {
            return;
        }
        // data_to_flush = buffer_->buffer_data_;
        data_to_flush.reserve(buffer_->buffer_data_.size());
        for (const auto& pair : buffer_->buffer_data_) {
            data_to_flush.push_back(pair.second);
        }
        buffer_->buffer_data_.clear();
        buffer_was_empty = false;
    }
    // if buffer is empty, we don't flush
    if (buffer_was_empty) {
        return;
    }

    // generate new level 0 SSTable id and file path
    uint64_t new_file_id = next_file_id_.fetch_add(1);
    std::string new_file_path = getFilePath(0, new_file_id);
    std::string bf_file_path = getBloomFilterPath(0, new_file_id);
    std::shared_ptr<SSTable> sstable_ptr = nullptr;

    try {
        // create the SSTable object and write to disk
        sstable_ptr = std::make_shared<SSTable>(data_to_flush, 0, new_file_path, bf_file_path);
        // std::cout << "[LSMTree] flushed buffer to new SSTable file: " << new_file_path << std::endl;
    } catch (const std::exception& e) {
        std::cerr << "can't create/write SSTable during flush: " << e.what() << std::endl;
        // If file creation failed revert file id
        next_file_id_--;
        // maybe throw error here? idk
        return;
    }

    // add the new SSTable pointer to level 0's list
    levels_[0]->addSSTable(sstable_ptr);

    // trigger compaction check before adding to Level 0 in memory
    // this step is now done in the compaction thread
    // {
    //     // acquire compaction lock, to ensure it's acquired one time
    //     std::lock_guard<std::mutex> compaction_lock(compaction_mutex_);
    //     this->checkCompaction(0);
    // }
}

// check if compaction is needed for the given level
void LSMTree::doCompactionCheck(size_t level_index) {
    // if last level or shutdown requested, ignore
    if (shutdown_requested_) {
        return;
    }
    if (level_index >= levels_.size()) {
        return;
    }

    bool needs_compaction = levels_[level_index]->needsCompaction();

    if (needs_compaction) {
        // only compact if not last level
        // check last level here instead of in compactLevel
        if (level_index < levels_.size() - 1) {
            {
                std::lock_guard lock(compaction_mutex_);
                compaction_tasks_.push(level_index);
            }
            // notify the compaction thread to do the compaction
            compaction_task_cv_.notify_one();
        }
    }
}

// compact the given level that needs compaction
void LSMTree::compactLevelHelper(size_t level_index) {
    if (!levels_[level_index]->needsCompaction()) {
        return;
    }
    size_t next_level_index = level_index + 1;
    std::vector<std::shared_ptr<SSTable>> input_tables_level;
    std::vector<std::shared_ptr<SSTable>> input_tables_level_next;

    // Select ALL tables from the current level (tier) for compaction
    // getSSTables locks level_mutex_
    {
        input_tables_level = levels_[level_index]->getSSTables();
    }
    if (input_tables_level.empty()) {
        std::cerr << "[LSMTree Compaction ERROR] No tables in current level " << level_index << " to compact." << std::endl;
        return;
    }

    // Perform merge logic - mergeSSTables now takes an empty input_tables_level_next
    std::vector<std::shared_ptr<SSTable>> output_tables;
    try {
        for (auto& table : input_tables_level) {
            if (!table->data_loaded_) {
                if (!table->loadFromDisk()) {
                    std::cerr << "Error loading input table " << table->file_path_ << " for merge." << std::endl;
                    throw std::runtime_error("Failed to load input SSTable for merge");
                }
            }
        }
        // mergeSSTables doesn't lock levels_ since it copies the data when merging them
        output_tables = mergeSSTables(input_tables_level,
                                      input_tables_level_next,
                                      next_level_index);
    } catch (const std::exception& e) {
        std::cerr << "Error during tiered SSTable merge: " << e.what() << std::endl;

        // clean up partially created files
        for (const auto& failed_output : output_tables) {
            deleteSSTableFile(failed_output);
        }
        return;
    }

    // 3. atomic replace in memory: protected by compaction_mutex_
    // removeAllSSTables locks level_mutex_
    // exclusive locks on levels motified
    levels_[level_index]->removeAllSSTables(input_tables_level);

    for (const auto& table : output_tables) {
        levels_[next_level_index]->addSSTable(table);
    }

    // TODO: updateHistory

    // delete the old SSTable files that were merged from the current level
    for (const auto& table : input_tables_level) {
        deleteSSTableFile(table);
    }

    // after compacted this level, compact the next if needed
    doCompactionCheck(next_level_index);
}

// Strictly for testing purposes
bool SSTable::keyInSSTable(int key) {
    if (!keyInRange(key)) { return false; }

    if (!data_loaded_) {
        if (!loadFromDisk()) {
        std::cerr << "can't load SSTable " << file_path_ << std::endl;
            return false;
        }
    }
    // Perform binary search
    // TODO: use fence pointers
    auto it = std::lower_bound(table_data_.begin(), table_data_.end(), static_cast<unsigned long>(key));
    return (it != table_data_.end() && it->key_ == key);
}

// print stats commands
// map is unique key:value pairs
// In lsm_tree.cpp
std::string LSMTree::print_stats() {
    // latest version and source for each key
    std::map<int, std::pair<DataPair, std::string>> logical_data_map;

    // buffer data collection
    {
        std::shared_lock lock(buffer_->buffer_mutex_);
        // for (const auto& dp : buffer_->buffer_data_) {
        //     logical_data_map.insert_or_assign(dp.key_, std::make_pair(dp, "BUF"));
        for (const auto& entry : buffer_->buffer_data_) {
            logical_data_map.insert_or_assign(entry.first, std::make_pair(entry.second, "BUF"));
        }
    }

    // level data collection
    for (size_t i = 0; i < levels_.size(); ++i) {
        const auto& level_ptr = levels_[i];
        std::string current_level_label = "L" + std::to_string(i + 1);

        std::vector<std::shared_ptr<SSTable>> sstables_from_level;
        {
            std::shared_lock lock(level_ptr->level_mutex_);
            sstables_from_level = level_ptr->sstables_;
        }
        std::reverse(sstables_from_level.begin(), sstables_from_level.end());

        for (const auto& sstable_ptr : sstables_from_level) {
            std::vector<DataPair> sstable_data_content;
            {
                std::lock_guard<std::mutex> sstable_lock(sstable_ptr->sstable_mutex_);
                if (!sstable_ptr->data_loaded_) {
                    if (!sstable_ptr->loadFromDisk()) {
                        std::cerr << "[STATS_ERROR] Failed to load SSTable " << sstable_ptr->file_path_
                                  << " for stats." << std::endl;
                        continue;
                    }
                }
                sstable_data_content = sstable_ptr->table_data_;
            }

            for (const auto& dp : sstable_data_content) {
                if (logical_data_map.find(dp.key_) == logical_data_map.end()) {
                    logical_data_map.insert_or_assign(dp.key_, std::make_pair(dp, current_level_label));
                }
            }
        }
    }

    // calculate total logical pairs (non-deleted)
    int total_logical_pairs = 0;
    // group data by source for printing KVL lines later AND for counts
    std::map<std::string, std::vector<DataPair>> data_grouped_by_source;
    // entry.first is key, entry.second is pair<DataPair, string_label>
    for (const auto& entry : logical_data_map) {
        if (!entry.second.first.deleted_) {
            total_logical_pairs++;
            data_grouped_by_source[entry.second.second].push_back(entry.second.first);
        }
    }

    // construct the output string
    std::stringstream result_ss;

    // logical pairs: first line
    result_ss << "Logical Pairs: " << total_logical_pairs;

    // counts for all sources on one line
    std::stringstream counts_line_ss;
    bool first_count_on_line = true;

    // Buffer count
    std::string buf_label = "BUF";
    if (data_grouped_by_source.count(buf_label) && !data_grouped_by_source[buf_label].empty()) {
        counts_line_ss << buf_label << ": " << data_grouped_by_source[buf_label].size();
        first_count_on_line = false;
    }

    // level counts
    for (size_t i = 0; i < levels_.size(); ++i) {
        std::string level_label = "L" + std::to_string(i + 1);
        if (data_grouped_by_source.count(level_label) && !data_grouped_by_source[level_label].empty()) {
            if (!first_count_on_line) {
                counts_line_ss << ", ";
            }
            counts_line_ss << level_label << ": " << data_grouped_by_source[level_label].size();
            first_count_on_line = false;
        }
    }

    std::string counts_line_str = counts_line_ss.str();
    if (!counts_line_str.empty()) {
        result_ss << "\n" << counts_line_str;
    }

    // KVL data, grouped by source, each group on its own line
    auto print_kvl_for_source = 
        [&](const std::string& source_label, const std::string& display_label) {
        if (data_grouped_by_source.count(source_label)) {
            const auto& pairs_from_source = data_grouped_by_source[source_label];
            if (!pairs_from_source.empty()) {
                result_ss << "\n";
                bool first_kvl_in_section = true;
                // print each key-value pair
                for (const auto& dp : pairs_from_source) { 
                    if (!first_kvl_in_section) {
                        result_ss << " ";
                    }
                    result_ss << dp.key_ << ":" << dp.value_ << ":" << display_label;
                    first_kvl_in_section = false;
                }
            }
        }
    };

    // Print KVL for Buffer
    print_kvl_for_source("BUF", "BUF");

    // Print KVL for Levels
    for (size_t i = 0; i < levels_.size(); ++i) {
        std::string level_label = "L" + std::to_string(i + 1);
        print_kvl_for_source(level_label, level_label);
    }

    return result_ss.str();
}