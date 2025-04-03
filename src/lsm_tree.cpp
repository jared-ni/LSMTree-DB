#include "lsm_tree.hh"
#include <iostream>
#include <queue>
#include <algorithm>


// helper function to generate SSTable filename
inline std::string generateSSTableFilename(uint64_t file_id) {
    std::stringstream ss;
    ss << std::setw(6) << std::setfill('0') << file_id << ".sst";
    return ss.str();
}

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
SSTable::SSTable(const std::vector<DataPair>& data, int level_num,
                 const std::string& file_path, const std::string& bf_file_path) :
    bloom_filter_(data.size())
{
    this->table_data_ = data;
    this->level_num_ = level_num;
    this->file_path_ = file_path;
    this->bf_file_path_ = bf_file_path;
    this->data_loaded_ = true;
    this->fence_ptr_count_ = 0;
    // add bloom filter
    // this->bloom_filter_ = BloomFilter(data.size());

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
        
        // TODO: bloom filter
        bloom_filter_ = BloomFilter(size_);
        for (const auto& dataPair : data) {
            bloom_filter_.add(dataPair.key_);
        }


        // set up fence pointers if needed, and one for remainder
        fence_ptr_count_ = size_ / FENCE_PTR_BLOCK_SIZE;
        if (size_ % FENCE_PTR_BLOCK_SIZE != 0) {
            fence_ptr_count_++;
        }
        // TODO: create fence pointers for binary search
        for (int i = 0; i < fence_ptr_count_; i++) {
            fence_ptr fp;
            // calculate the min and max key for this block
            int start_index = i * FENCE_PTR_BLOCK_SIZE;
            int end_index = std::min(start_index + FENCE_PTR_BLOCK_SIZE - 1, static_cast<int>(size_ - 1));
            fp.min_key = data[start_index].key_;
            fp.max_key = data[end_index].key_;
            fp.file_offset = start_index;
            fence_pointers_.push_back(fp);
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
    this->min_key_ = std::numeric_limits<long>::max();
    this->max_key_ = std::numeric_limits<long>::min();

    this->fence_ptr_count_ = 0;
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
    long key, value;
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
        min_key_ = std::numeric_limits<long>::max();
        max_key_ = std::numeric_limits<long>::min();
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

    return true;
}



void SSTable::printSSTable() const {
    if (!data_loaded_) {
        std::cout << "(SSTable on disk: " << file_path_ << " size: " << size_
                  << " range: [" << min_key_ << "," << max_key_ << "]) ";
        return;
    }
    for (int i = 0; i < table_data_.size(); i++) {
        std::cout << table_data_[i].key_ << ":" << table_data_[i].value_ << ", ";
    }
}

void SSTableSnapshot::printSSTable() const {
    for (int i = 0; i < table_data.size(); i++) {
        std::cout << table_data[i].key_ << ":" << table_data[i].value_ << ", ";
    }
}

std::optional<DataPair> SSTable::getDataPair(long key) {
    if (!keyInRange(key)) {
        return std::nullopt;
    }

    // persistence check: if data not loaded, load from disk
    if (!data_loaded_) {
        // load from disk
        if (!loadFromDisk()) {
            std::cerr << "[SSTable] failed to load SSTable from disk: " << file_path_ << std::endl;
            return std::nullopt;
        }
    }
    // replace with binary search
    auto it = std::lower_bound(table_data_.begin(), table_data_.end(), key);
    if (it != table_data_.end() && it->key_ == key) {
        if (it->deleted_) {
            return std::nullopt;
        } else {
            return *it;
        }
    }
    return std::nullopt;
}

bool SSTable::keyInRange(long key) const {
    return key >= min_key_ && key <= max_key_;
}

bool SSTable::keyInSSTable(long key) {
    if (!keyInRange(key)) { return false; }

    if (!data_loaded_) {
        if (!loadFromDisk()) {
        std::cerr << "can't load SSTable " << file_path_ << std::endl;
            return false;
        }
    }
    // Perform binary search
    // TODO: use fence pointers
    auto it = std::lower_bound(table_data_.begin(), table_data_.end(), key);
    return (it != table_data_.end() && it->key_ == key);
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
    return (cur_table_count_ >= table_capacity_);
    // || (cur_total_entries_ >= entries_capacity_);
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
}

void LSMTree::setupDB() {
    std::error_code ec;
    // makes sure path looks good
    if (!std::filesystem::exists(db_path_) || 
        !std::filesystem::is_directory(db_path_)) {
        // std::cout << "[LSMTree] no database directory, creating at: " << db_path_ << std::endl;
        if (!std::filesystem::create_directories(db_path_, ec)) {
            // Fatal error if DB directory cannot be created
            std::cerr << "Failed creating db dir " << db_path_ << ": " << ec.message() << std::endl;
            throw std::runtime_error("failed to create DB directory");
        }
    }

    // set up levels
    for (size_t i = 0; i < total_levels_; ++i) {
        // returns the path for each level, given level num
        std::string level_path = getLevelPath(i);
        if (!std::filesystem::exists(level_path)) {
            // std::cout << "[LSMTree] creating level directory: " << level_path << std::endl;
            if (!std::filesystem::create_directory(level_path, ec)) {
                std::cerr << "failed to create level directory " << level_path << ": " << ec.message() << std::endl;
                throw std::runtime_error("failed to create level directory");
            }
            if (!std::filesystem::create_directory(level_path + "/bloom_filters", ec)) {
                std::cerr << "failed to create bloom filter directory " << level_path + "/bloom_filters" 
                          << ": " << ec.message() << std::endl;
                throw std::runtime_error("failed to create bloom filter directory");
            }
        }
    }

    // check history to load existing state if there's any
    if (std::filesystem::exists(history_path_)) {
        // std::cout << "[LSMTree] loading history from: " << history_path_ << std::endl;
        loadHistory();
    } else {
        // std::cout << "[LSMTree] no history found at: " << history_path_ << std::endl;
        // If no history, initialize an empty history file
        // reset the next_file_id_ to 1
        next_file_id_ = 1;
        std::ofstream history_file(history_path_);
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
void LSMTree::loadHistory() {
    // std::ifstream infile(history_path_);
    // if (!infile) {
    //     std::cerr << "can't open history file: " << history_path_ << std::endl;
    //     throw std::runtime_error("failed to load history");
    // }

    // std::string line;
    // std::string command;
    // int level_num;
    // int file_id;
    // long min_key;
    // long max_key;
    // size_t size;
    // int next_id_val;

    // std::map<int, std::map<uint64_t, std::shared_ptr<SSTable>>> active_tables;
    // uint64_t max_seen_id = 0;

    // while (std::getline(infile, line)) {
    //     std::stringstream ss(line);
    //     ss >> command;

    //     if (command == "ADD") {
    //         // Format - ADD level file_id min_key max_key size
    //         if (ss >> level_num >> file_id >> min_key >> max_key >> size) {
    //             if (level_num < 0 || level_num >= levels_.size()) {
    //                 std::cerr << "history Error: invalid level " << level_num << " in line: " << line << std::endl; 
    //                 continue;
    //             }
    //             std::string file_path = getFilePath(level_num, file_id);
    //             // check if the file exists on disk
    //             if (!std::filesystem::exists(file_path)) {
    //                 std::cerr << "history Error: file " << file_path << " not found but listed in history!" << std::endl;
    //                 // Should I skip or error out here?
    //                 continue;
    //             }

    //             auto sstable = std::make_shared<SSTable>(level_num, file_path);
    //             // load into metadata using history log parsed
    //             sstable->min_key_ = min_key;
    //             sstable->max_key_ = max_key;
    //             sstable->size_ = size;
    //             sstable->data_loaded_ = false;

    //             active_tables[level_num][file_id] = sstable;
    //             if (file_id > max_seen_id) {
    //                 max_seen_id = file_id;
    //             }
    //         } else {
    //              std::cerr << "history: bad add on line " << line << std::endl;
    //         }
    //     } else if (command == "REMOVE") {
    //     // might have crashed before logging additions if so remove ref
    //         if (ss >> level_num >> file_id) {
    //             if (active_tables.count(level_num)) {
    //                 active_tables[level_num].erase(file_id);
    //             }
    //         } else {
    //             std::cerr << "history bad REMOVE: " << line << std::endl;
    //         }
    //     } else if (command == "NEXT_ID") {
    //         // Format - NEXT_ID id_value
    //         if (ss >> next_id_val) {
    //             next_file_id_ = next_id_val;
    //         } else {
    //             std::cerr << "history bad NEXT_ID: " << line << std::endl;
    //         }
    //     } else {
    //         if (!line.empty() && line[0] != '#') {
    //             std::cerr << "history Unknown command '" << command << ": " << line << std::endl;
    //         }
    //     }
    // }

    // infile.close();

    // //populate LSM tree levels_ from the processed active_tables map
    // for (auto const& [level_idx, tables_map] : active_tables) {
    //     if (level_idx >= 0 && level_idx < levels_.size()) {
    //         for (auto const& [f_id, sstable_ptr] : tables_map) {
    //             levels_[level_idx]->addSSTable(sstable_ptr);
    //             std::cout << "[LSMTree] loaded SSTable " << sstable_ptr->file_path_ << " to level " << level_idx << std::endl;
    //         }
    //         // do i need to sort tables? (L0 by creation time/ID, L1+ by key range)
    //     }
    // }

    //  // adjust the next_file_id_
    // if (next_file_id_ <= max_seen_id) {
    //     std::cout << "[LSMTree Load] Adjusting next_file_id_ from " << next_file_id_ << " to " << (max_seen_id + 1) << std::endl;
    //     next_file_id_ = max_seen_id + 1;
    // }
    // if (next_file_id_ == 0) {
    //     next_file_id_ = 1;
    // }

    // std::cout << "[LSMTree] State loaded. Next file ID will be: " << next_file_id_ << std::endl;
}

void LSMTree::updateHistory(
    const std::vector<std::shared_ptr<SSTable>>& to_remove_l,
    const std::vector<std::shared_ptr<SSTable>>& to_remove_l_next,
    const std::vector<std::shared_ptr<SSTable>>& to_add_l_next) {

//     std::string history_temp_path = history_path_ + ".tmp";
//     std::ofstream outfile(history_temp_path);
//     if (!outfile) {
//         std::cerr << "[LSMTree] error opening history temp file " << history_temp_path << std::endl;
//         return;
//     }

//     // write current state to temp file
//     for (const auto& level_ptr : levels_) {
//         const auto current_tables = level_ptr->getSSTables();
//         for (const auto& table : current_tables) {
//             // Check if this table is not being removed in current operation
//             bool being_removed = false;
//             for(const auto& rem_t : to_remove_l) if (rem_t == table) being_removed = true;
//             if (!being_removed) {
//                 for(const auto& rem_t : to_remove_l_next) {
//                     if (rem_t == table) {
//                         being_removed = true;
//                     }
//                 }
//             }

//             if (!being_removed) {
//                 uint64_t file_id = std::stoull(std::filesystem::path(table->file_path_).stem().string());
//                 outfile << "ADD " << table->level_num_ << " " << file_id << " "
//                        << table->min_key_ << " " << table->max_key_ << " " << table->size_ << "\n";
//             }
//         }
//     }
//     // new additions
//     for (const auto& table : to_add_l_next) {
//         if (table) {
//             uint64_t file_id = std::stoull(std::filesystem::path(table->file_path_).stem().string());
//             outfile << "ADD " << table->level_num_ << " " << file_id << " "
//                    << table->min_key_ << " " << table->max_key_ << " " << table->size_ << "\n";
//         }
//     }
//     // write next_id
//     outfile << "NEXT_ID " << next_file_id_ << "\n";
//     outfile.flush();
//     outfile.close();
//     int fd = open(temp_history_path.c_str(), O_WRONLY);
//     if (fd != -1) { fsync(fd); close(fd); }

//     if (outfile.fail()) {
//         std::cerr << "failed to write or close temp History: " << temp_history_path << std::endl;
//         std::filesystem::remove(temp_history_path);
//         throw std::runtime_error("Failed write during History update");
//     }

//     // replace the original history file with the temp file
//     std::error_code ec;
//     std::filesystem::rename(temp_history_path, history_path, ec);
//     if (ec) {
//         throw std::runtime_error("Failed atomic history update via rename");
//     }

//     std::cout << "[LSMTree] atomic rename of history." << std::endl;
// }

// void LSMTree::updateHistoryAdd(std::shared_ptr<SSTable> new_table) {
//     // Call the main update function with empty removal lists
//     std::vector<std::shared_ptr<SSTable>> empty_removals;
//     std::vector<std::shared_ptr<SSTable>> additions = {new_table};
//     updateHistory(empty_removals, empty_removals, additions);
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
    // lock mutex
    // std::lock_guard<std::mutex> lock(flush_mutex_);
    // flush buffer to level 0
    if (buffer_->buffer_data_.empty()) {
        return;
    }
    // std::cout << "[LSMTree] Flushing buffer..." << std::endl;

    // data from buffer
    // this performs a deep copy of the buffer data to ensure we have a snapshot of the data to flush
    std::vector<DataPair> data_to_flush = buffer_->buffer_data_;
    // TODO: 
    buffer_->buffer_data_.clear();
    buffer_->cur_size_ = 0;

    // generate new level 0 SSTable id and file path
    uint64_t new_file_id = next_file_id_++;
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
    checkCompaction(0);

    // std::cout << "[LSMTree] Flushed SSTable " << new_file_id << " to Level 0" << std::endl;
    // levels_[0]->printLevel();
}

bool LSMTree::putData(const DataPair& data) {
    bool rt = buffer_->putData(data);
    // if level is full, flush. put data in buffer either way
    if (buffer_->isFull()) {
        flushBuffer();
    }

    return rt;
}

std::optional<DataPair> LSMTree::getData(long key) {
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
        const auto& cur_sstables = cur_level->getSSTables();

        for (const auto& cur_sstable : cur_sstables) {
            // TODO: check bloom filter
            if (!cur_sstable->bloom_filter_.might_contain(key)) {
                continue;
            }

            // check range: if not in range, continue
            if (!cur_sstable->keyInRange(key)) {
                continue;
            }
            // TODO: now actually search the sstable, using fence pointers
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

// TODO: need to implement still
void LSMTree::rangeData(long low, long high) {
    for (int i = low; i < high; i++) {
        std::optional<DataPair> data_pair = getData(i);
        if (data_pair.has_value()) {
            // std::cout << i << ":" << data_pair.value().value_ << std::endl;
        } else {
            // std::cout << std::endl;
        }
    }
}

// delete is just putting in the tombstone in the buffer for now
bool LSMTree::deleteData(long key) {
    // mark data in buffer as tombstone, if found
    DataPair tombstone_data(key, 0, true);
    bool success = buffer_->putData(tombstone_data);
    return success;
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
    auto& cur_level = levels_[level_index];
    // need to flush first before adding new sstable if current level is full

    bool needs_compaction = cur_level->needsCompaction();
    if (needs_compaction) {
        // std::cout << "[LSMTree] Level " << level_index << " needs compaction..." << std::endl;
        compactLevel(level_index);
    }
    return true;
}

// basic tiering
void LSMTree::compactLevel(size_t level_index) {
    // TODO: coarse grained lock for now
    // std::lock_guard<std::mutex> compaction_lock(compaction_mutex_);

    // Check needsCompaction status within the lock if using fine-grained locking later.
    // For now, we re-check, similar to before.
    {
        // std::shared_lock<std::shared_mutex> lock(levels_[level_index]->mutex_);
        // Use the existing needsCompaction which might use >= threshold
        if (!levels_[level_index]->needsCompaction()) {
            // std::cout << "[LSMTree] Race condition or no longer needs compaction for Level " << level_index << ". Skipping." << std::endl;
            return;
        }
    }

    size_t next_level_index = level_index + 1;
    if (next_level_index >= levels_.size()) {
        // std::cout << "[LSMTree] Level " << level_index << " is the last level, no compaction" << std::endl;
        // Tiered compaction usually stops at the last level or has a different strategy there.
        // For now, we just stop.
        return;
    }

    // std::cout << "[LSMTree Tiered] starting compaction for level " << level_index << " into level " << next_level_index << std::endl;

    // compaction participants
    std::vector<std::shared_ptr<SSTable>> input_tables_level;
    std::vector<std::shared_ptr<SSTable>> input_tables_level_next; // Will remain empty for basic tiering

    { // Scope for reading level contents (needs locking if concurrent)

        // Select ALL tables from the current level (tier) for compaction
        input_tables_level = levels_[level_index]->getSSTables();

        // Ensure the level wasn't empty if needsCompaction was true
        if (input_tables_level.empty()) {
             std::cerr << "[LSMTree Tiered] Compaction triggered on empty level " << level_index << "? Aborting." << std::endl;
             return;
        }

        // In basic tiering, we don't select overlapping tables from the next level as direct merge input.
        input_tables_level_next.clear();

    } // End scope for reading levels


    // Abort if somehow no tables were selected (shouldn't happen after the check above)
    if (input_tables_level.empty()) {
        // std::cout << "[LSMTree Tiered] No input tables selected from L" << level_index << ". Aborting." << std::endl;
        return;
    }

    // std::cout << "[LSMTree Tiered] Merging " << input_tables_level.size() << " tables from Tier " << level_index
    //           << " into Tier " << next_level_index << std::endl; // Modified log message slightly

    // Perform merge logic - mergeSSTables now takes an empty input_tables_level_next
    std::vector<std::shared_ptr<SSTable>> output_tables;
    try {
        output_tables = mergeSSTables(input_tables_level,
                                     input_tables_level_next, // Pass the empty vector
                                     next_level_index);
    } catch (const std::exception& e) {
         std::cerr << "Error during tiered SSTable merge: " << e.what() << std::endl;
         // Handle error, potentially clean up partial output files if possible
         return; // Abort compaction on merge error
    }


    // atomic replace in memory
    {
        // std::unique_lock<std::shared_mutex> lock1(levels_[level_index]->mutex_);
        // std::unique_lock<std::shared_mutex> lock2(levels_[next_level_index]->mutex_);

        // Remove ALL input tables from the current level (tier)
        levels_[level_index]->removeAllSSTables(input_tables_level);
        levels_[next_level_index]->removeAllSSTables(input_tables_level_next);
        for (const auto& table : output_tables) {
            levels_[next_level_index]->addSSTable(table);
        }
    }
    // delete the SSTable files that were merged from the current level
    for (const auto& table : input_tables_level) {
        deleteSSTableFile(table);
    }

    // std::cout << "[LSMTree Tiered] finished compaction, added " << output_tables.size()
    //           << " new tables to Tier " << next_level_index << "." << std::endl;
    // levels_[level_index]->printLevel();
    // levels_[next_level_index]->printLevel();

    // after compacted this level, compact the next if needed
    checkCompaction(next_level_index);
}

// merge function for two SSTables
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
        if (!all_inputs[i]->data_loaded_) {
            if (!all_inputs[i]->loadFromDisk()) {
                 std::cerr << "Error loading input table " << all_inputs[i]->file_path_ << " for merge." << std::endl;
                 throw std::runtime_error("Failed to load input SSTable for merge");
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

    long last_key = std::numeric_limits<long>::min();
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
        bool is_last_level = (output_level_num == (levels_.size() - 1));
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