#include "lsm_tree.hh"
#include <iostream>
#include <vector>
#include <cassert>
#include <string>
#include <memory>
#include <limits>
#include <filesystem>
#include <system_error>

// Define a temporary directory for SSTable unit tests
const std::string TEMP_SSTABLE_DIR = "test_sstable_temp_files";

// Helper function to create the temporary directory
void create_temp_dir(const std::string& dir) {
    std::error_code ec;
    std::filesystem::remove_all(dir, ec);
    if (!std::filesystem::create_directory(dir, ec)) {
        if (ec && ec != std::errc::file_exists) {
             std::cerr << "Warning: Could not create temp directory " << dir << ": " << ec.message() << std::endl;
        }
    }
}

// Helper function to remove the temporary directory
void remove_temp_dir(const std::string& dir) {
    std::error_code ec;
    std::filesystem::remove_all(dir, ec);
    if (ec) {
        std::cerr << "Warning: Could not remove temp directory " << dir << ": " << ec.message() << std::endl;
    }
}

// DataPair tests
void test_datapair() {
    std::cout << "[TEST] Testing DataPair ------------" << std::endl;
    // 1. test DataPair constructor
    DataPair data_pair(1, 10);
    assert(data_pair.key_ == 1);
    assert(data_pair.value_ == 10);
    assert(!data_pair.deleted_);
    std::cout << "DataPair constructor tests PASSED." << std::endl;

    // 2. test DataPair comparison
    assert(data_pair < 2);
    assert(data_pair < DataPair(2, 20));
    assert(data_pair == DataPair(1, 10));
    std::cout << "DataPair comparison tests PASSED." << std::endl;
}

void test_sstable() {
    std::cout << "[TEST] Testing SSTable ------------" << std::endl;
    create_temp_dir(TEMP_SSTABLE_DIR); // Create directory for test files

    // 1. test empty SSTable
    std::vector<DataPair> empty_data;
    // Need a valid path for the constructor to write to
    std::string empty_table_path = TEMP_SSTABLE_DIR + "/empty_table.sst";
    // Use try-catch as constructor now throws on write failure
    try {
        SSTable empty_table(empty_data, 1, empty_table_path);
        assert(empty_table.size_ == 0);
        assert(empty_table.min_key_ == std::numeric_limits<long>::max());
        assert(empty_table.max_key_ == std::numeric_limits<long>::min());
        assert(!empty_table.keyInRange(10));
        // getDataPair is non-const now because of potential loadFromDisk
        assert(!empty_table.getDataPair(10).has_value());
        std::cout << "Empty SSTable tests PASSED." << std::endl;
    } catch (const std::exception& e) {
        std::cerr << "Empty SSTable test FAILED with exception: " << e.what() << std::endl;
        assert(false); // Force test failure
    }

    // 2. test SSTable with data
    std::vector<DataPair> data = {
        DataPair(1, 10),
        DataPair(2, 20),
        DataPair(3, 30),
        DataPair(4, 40),
        DataPair(5, 50)
    };
    std::string table_path = TEMP_SSTABLE_DIR + "/data_table.sst";
    try {
        // data and level1 SSTable
        SSTable table(data, 1, table_path);
        assert(table.size_ == 5);
        assert(table.min_key_ == 1);
        assert(table.max_key_ == 5);
        std::cout << "SSTable min max key test PASSED." << std::endl;

        // 3. test keyInRange
        assert(table.keyInRange(1));
        assert(table.keyInRange(5));
        assert(!table.keyInRange(0));
        assert(!table.keyInRange(6));

        // 4. test keyInSSTable (now non-const)
        assert(table.keyInSSTable(1));
        assert(table.keyInSSTable(5));
        assert(!table.keyInSSTable(0));
        assert(!table.keyInSSTable(6));

        // 5. test getDataPair (now non-const)
        std::optional<DataPair> data_pair_option;
        data_pair_option = table.getDataPair(1);
        assert(data_pair_option.has_value());
        assert(data_pair_option.value().key_ == 1);
        assert(data_pair_option.value().value_ == 10);
        data_pair_option = table.getDataPair(5);
        assert(data_pair_option.has_value());
        assert(data_pair_option.value().key_ == 5);
        assert(data_pair_option.value().value_ == 50);
        data_pair_option = table.getDataPair(0);
        assert(!data_pair_option.has_value());
        std::cout << "SSTable keyInRange, keyInSSTable, getDataPair tests PASSED." << std::endl;

        // 6. test loadFromDisk functionality (optional but good)
        // Create a new SSTable object pointing to the same file, but unloaded
        SSTable table_to_load(1, table_path); // Use the placeholder constructor
        // Manually set metadata as MANIFEST would normally do this
        table_to_load.min_key_ = 1;
        table_to_load.max_key_ = 5;
        table_to_load.size_ = 5; // Assume we know this
        assert(!table_to_load.data_loaded_);
        // Accessing data should trigger load
        auto loaded_pair = table_to_load.getDataPair(3);
        assert(table_to_load.data_loaded_);
        assert(loaded_pair.has_value());
        assert(loaded_pair.value().key_ == 3);
        assert(loaded_pair.value().value_ == 30);
        std::cout << "SSTable loadFromDisk test PASSED." << std::endl;


    } catch (const std::exception& e) {
        std::cerr << "SSTable with data test FAILED with exception: " << e.what() << std::endl;
        assert(false); // Force test failure
    }

    remove_temp_dir(TEMP_SSTABLE_DIR); // Clean up test files
}

// Level tests
void test_level() {
    const size_t table_capacity = 2;
    const size_t entries_capacity = 4;

    std::cout << "[TEST] Testing Level ------------" << std::endl;
    create_temp_dir(TEMP_SSTABLE_DIR);

    // 1. test create level 1 with capacity 2
    Level level(1, table_capacity, entries_capacity);
    assert(level.level_num_ == 1);
    assert(level.table_capacity_ == table_capacity);
    assert(level.entries_capacity_ == entries_capacity);
    assert(level.cur_table_count_ == 0);
    assert(level.cur_total_entries_ == 0);
    assert(level.getSSTables().empty());
    assert(!level.needsCompaction());
    std::cout << "Level constructor tests PASSED." << std::endl;

    // 2. test addSSTable
    std::vector<DataPair> data1 = {
        DataPair(1, 10),
        DataPair(2, 20)
    };
    std::vector<DataPair> data2 = {
        DataPair(3, 30),
        DataPair(4, 40)
    };
    //need paths for the constructors
    std::string sstable1_path = TEMP_SSTABLE_DIR + "/level_sstable1.sst";
    std::string sstable2_path = TEMP_SSTABLE_DIR + "/level_sstable2.sst";
    // define sstable3 path here too
    std::string sstable3_path = TEMP_SSTABLE_DIR + "/level_sstable3.sst";


    try {
        std::shared_ptr<SSTable> sstable1 = std::make_shared<SSTable>(data1, 1, sstable1_path);
        std::shared_ptr<SSTable> sstable2 = std::make_shared<SSTable>(data2, 1, sstable2_path);

        level.addSSTable(sstable1);
        assert(level.cur_table_count_ == 1);
        assert(level.cur_total_entries_ == 2);
        assert(level.getSSTables().size() == 1);
        assert(level.getSSTables()[0] == sstable1);
        assert(!level.needsCompaction());

        level.addSSTable(sstable2);
        assert(level.cur_table_count_ == 2);
        assert(level.cur_total_entries_ == 4);
        assert(level.getSSTables().size() == 2);
        assert(level.getSSTables()[0] == sstable1);
        assert(level.getSSTables()[1] == sstable2);
        // Compaction triggered by both table count and entry count matching capacity
        assert(level.needsCompaction());
        std::cout << "Level addSSTable tests PASSED." << std::endl;

        // 3. test removeSSTable
        level.removeSSTable(sstable1);
        assert(level.cur_table_count_ == 1);
        assert(level.cur_total_entries_ == 2); // sstable2 remains
        assert(level.getSSTables().size() == 1);
        assert(level.getSSTables()[0] == sstable2);
        assert(!level.needsCompaction());
        std::cout << "Level removeSSTable tests PASSED." << std::endl;

        // 4. test needsCompaction
        level.addSSTable(sstable1);
        assert(level.cur_table_count_ == 2);
        assert(level.cur_total_entries_ == 4);
        assert(level.needsCompaction());
        std::cout << "Level needsCompaction tests PASSED." << std::endl;

        // 5. add same sstable to level: currently it's possible (and count increases)
        level.addSSTable(sstable1);
        assert(level.cur_table_count_ == 3);
        assert(level.cur_total_entries_ == 6);
        assert(level.needsCompaction());
        level.printLevel();
        level.removeSSTable(sstable1);
        assert(level.cur_table_count_ == 2);
        assert(level.cur_total_entries_ == 4);
        level.printLevel();
        std::cout << "Level printLevel tests PASSED." << std::endl;

        // 6. test getSSTables
        std::vector<std::shared_ptr<SSTable>> sstables = level.getSSTables();
        assert(sstables.size() == 2); // sstable2 and the second sstable1
        assert(sstables[0] == sstable2);
        // The second element should be the pointer to sstable1
        assert(sstables[1] == sstable1);
        std::cout << "Level getSSTables tests PASSED." << std::endl;

        // 7. test removing non-existent sstable: nothing should happen
        std::shared_ptr<SSTable> sstable3 = std::make_shared<SSTable>(data1, 1, sstable3_path); // Create a different object
        size_t count_before = level.cur_table_count_;
        size_t entries_before = level.cur_total_entries_;
        level.removeSSTable(sstable3);
        level.printLevel();
        assert(level.cur_table_count_ == count_before);
        assert(level.cur_total_entries_ == entries_before);
        std::cout << "Level remove non-existent sstable tests PASSED." << std::endl;

    } catch (const std::exception& e) {
         std::cerr << "Level tests FAILED with exception: " << e.what() << std::endl;
         assert(false);
    }

    remove_temp_dir(TEMP_SSTABLE_DIR); // Clean up test files
}

// Buffer tests
// void test_buffer() {
//     std::cout << "[TEST] Testing Buffer ------------" << std::endl;

//     // 1. test create buffer with default capacity
//     Buffer buffer;
//     assert(buffer.capacity_ == BUFFER_CAPACITY);
//     assert(buffer.cur_size_ == 0);
//     std::cout << "Buffer constructor tests PASSED." << std::endl;

//     // 2. test add data to buffer
//     buffer.putData(DataPair(1, 10));
//     assert(buffer.cur_size_ == 1);
//     buffer.putData(DataPair(2, 20));
//     assert(buffer.cur_size_ == 2);
//     std::cout << "Buffer putData tests PASSED." << std::endl;

//     // 3. test get data from buffer
//     std::optional<DataPair> data_pair_option;
//     data_pair_option = buffer.getData(1);
//     assert(data_pair_option.has_value());
//     assert(data_pair_option.value().key_ == 1);
//     assert(data_pair_option.value().value_ == 10);
//     data_pair_option = buffer.getData(2);
//     assert(data_pair_option.has_value());
//     assert(data_pair_option.value().key_ == 2);
//     assert(data_pair_option.value().value_ == 20);
//     data_pair_option = buffer.getData(3);
//     assert(!data_pair_option.has_value());
//     std::cout << "Buffer getData tests PASSED." << std::endl;

//     // 4. test put same key in buffer
//     assert(buffer.getData(1).value().value_ == 10);
//     buffer.putData(DataPair(1, 100));
//     assert(buffer.cur_size_ == 2);
//     assert(buffer.getData(1).value().value_ == 100);
//     std::cout << "Buffer put same key tests PASSED." << std::endl;
// }

// LSM Tree tests
// void test_lsm_tree() {
//     std::cout << "[TEST] testing LSMTree ------------" << std::endl;
//     // Use smaller caps
//     const size_t TEST_BUFFER_CAP = 2;
//     const size_t BASE_LEVEL_TABLE_CAP = 2; // for l0
//     const size_t TEST_LEVEL_RATIO = 1; // l1 is 2* 2 = 4
//     const size_t total_levels = 4;

//     // 1. create LSMTree
//     LSMTree lsm_tree("test_db_simple", 
//                     TEST_BUFFER_CAP, 
//                     BASE_LEVEL_TABLE_CAP, 
//                     total_levels, 
//                     TEST_LEVEL_RATIO);
//     std::cout << "LSMTree created with BufferCap=" << TEST_BUFFER_CAP
//               << ", L0TableCap=" << BASE_LEVEL_TABLE_CAP << std::endl;

//     // 2. initial Puts (Fill Buffer)
//     lsm_tree.putData({1, 100}); // {1}
//     lsm_tree.putData({2, 200}); // {1, 2}
//     assert(lsm_tree.buffer_->cur_size_ == 2);
//     lsm_tree.putData({3, 150}); // flush to l0, {3}, l0 = 1 table
//     assert(lsm_tree.buffer_->cur_size_ == 1);
//     assert(lsm_tree.levels_[0]->cur_table_count_ == 1);
//     lsm_tree.putData({4, 250}); // {3, 4}

//     lsm_tree.buffer_->printBuffer();
//     lsm_tree.levels_[0]->printLevel();
//     lsm_tree.levels_[1]->printLevel();

//     lsm_tree.putData({5, 300}); // {5}, l0 = 2 tables
//     lsm_tree.putData({6, 350}); // {5, 6}
//     assert(lsm_tree.levels_[0]->cur_table_count_ == 2);

//     lsm_tree.putData({7, 400});
//     lsm_tree.putData({8, 450}); 
//     lsm_tree.putData({9, 500}); 
//     lsm_tree.putData({10, 550});
//     lsm_tree.putData({11, 600}); // {11}, l0 = 1 table, l1 = 2 tables
//     lsm_tree.putData({12, 650}); // {11, 12}
//     lsm_tree.putData({13, 700}); // flush to l0, {13}, l0 2 table, l1 2 tables
//     lsm_tree.putData({14, 750}); // {13, 14}
//     lsm_tree.putData({15, 800}); // {15}, l0 1 table, l1 1 tables, l2 1 table
//     lsm_tree.putData({16, 850}); // {15, 16}
//     lsm_tree.putData({17, 900}); // {17}, l0 2 table, l1 1 table, l2 1 table
//     lsm_tree.putData({18, 950}); // {17, 18}
//     lsm_tree.putData({19, 1000}); // {19}, l0 1 table, l1 2 table, l2 2 table

//     std::cout << "after 19, 1000" << std::endl;
//     lsm_tree.buffer_->printBuffer();
//     lsm_tree.levels_[0]->printLevel();
//     lsm_tree.levels_[1]->printLevel();
//     lsm_tree.levels_[2]->printLevel();
//     lsm_tree.levels_[3]->printLevel();

//     assert(lsm_tree.levels_[0]->cur_table_count_ == 1);
//     assert(lsm_tree.levels_[1]->cur_table_count_ == 2);
//     assert(lsm_tree.levels_[2]->cur_table_count_ == 2);
// }

int main() {
    test_datapair();
    test_sstable();
    test_level();
    // test_buffer();
    // test_lsm_tree();
    std::cout << "All tests passed!" << std::endl;
    return 0;
}
