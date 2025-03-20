#include "lsm_tree.hh"
#include <iostream>
#include <vector>
#include <cassert>
#include <string>
#include <memory>

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

// SSTable tests
void test_sstable() {
    std::cout << "[TEST] Testing SSTable ------------" << std::endl;

    // 1. test empty SSTable
    std::vector<DataPair> empty_data;
    SSTable empty_table(empty_data, 1);
    assert(empty_table.size_ == 0);
    assert(empty_table.min_key_ == std::numeric_limits<long>::max());
    assert(empty_table.max_key_ == std::numeric_limits<long>::min());
    assert(!empty_table.keyInRange(10));
    assert(!empty_table.getDataPair(10).has_value());
    std::cout << "Empty SSTable tests PASSED." << std::endl;

    // 2. test SSTable with data
    std::vector<DataPair> data = {
        DataPair(1, 10),
        DataPair(2, 20),
        DataPair(3, 30),
        DataPair(4, 40),
        DataPair(5, 50)
    };
    // data and level1 SSTable
    SSTable table(data, 1);
    assert(table.size_ == 5);
    assert(table.min_key_ == 1);
    assert(table.max_key_ == 5);
    std::cout << "SSTable min max key test PASSED." << std::endl;

    // 3. test keyInRange
    assert(table.keyInRange(1));
    assert(table.keyInRange(5));
    assert(!table.keyInRange(0));
    assert(!table.keyInRange(6));

    // 4. test keyInSSTable
    assert(table.keyInSSTable(1));
    assert(table.keyInSSTable(5));
    assert(!table.keyInSSTable(0));
    assert(!table.keyInSSTable(6));

    // 5. test getDataPair
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
}

// Level tests
void test_level() {
    const size_t level_capacity = 2;

    std::cout << "[TEST] Testing Level ------------" << std::endl;

    // 1. test create level 1 with capacity 2
    Level level(1, level_capacity);
    assert(level.level_num_ == 1);
    assert(level.capacity_ == level_capacity);
    assert(level.cur_size_ == 0);
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
    std::shared_ptr<SSTable> sstable1 = std::make_shared<SSTable>(data1, 1);
    std::shared_ptr<SSTable> sstable2 = std::make_shared<SSTable>(data2, 1);
    level.addSSTable(sstable1);
    assert(level.cur_size_ == 1);
    assert(level.getSSTables().size() == 1);
    assert(level.getSSTables()[0] == sstable1);
    assert(!level.needsCompaction());

    level.addSSTable(sstable2);
    assert(level.cur_size_ == 2);
    assert(level.getSSTables().size() == 2);
    assert(level.getSSTables()[0] == sstable1);
    assert(level.getSSTables()[1] == sstable2);
    assert(level.needsCompaction());
    std::cout << "Level addSSTable tests PASSED." << std::endl;

    // 3. test removeSSTable
    level.removeSSTable(sstable1);
    assert(level.cur_size_ == 1);
    assert(level.getSSTables()[0] == sstable2);
    assert(!level.needsCompaction());
    std::cout << "Level removeSSTable tests PASSED." << std::endl;

    // // 4. test needsCompaction
    level.addSSTable(sstable1);
    assert(level.needsCompaction());
    std::cout << "Level needsCompaction tests PASSED." << std::endl;

    // // 5. add same sstable to level: currently it's possible
    level.addSSTable(sstable1);
    level.printLevel();
    level.removeSSTable(sstable1); // this removes all of them currently
    level.printLevel();
    std::cout << "Level printLevel tests PASSED." << std::endl;

    // // 6. test getSSTables
    std::vector<std::shared_ptr<SSTable>> sstables = level.getSSTables();
    assert(sstables.size() == 2);
    assert(sstables[0] == sstable2);
    std::cout << "Level getSSTables tests PASSED." << std::endl;

    // 7. test removing non-existent sstable: nothing should happen
    std::shared_ptr<SSTable> sstable3 = std::make_shared<SSTable>(data1, 1);
    level.removeSSTable(sstable3);
    level.printLevel();
    assert(level.cur_size_ == 2);
    assert(level.getSSTables().size() == 2);
    std::cout << "Level remove non-existent sstable tests PASSED." << std::endl;
}

// Buffer tests
void test_buffer() {
    std::cout << "[TEST] Testing Buffer ------------" << std::endl;

    // 1. test create buffer with default capacity
    Buffer buffer;
    assert(buffer.capacity_ == BUFFER_CAPACITY);
    assert(buffer.cur_size_ == 0);
    std::cout << "Buffer constructor tests PASSED." << std::endl;

    // 2. test add data to buffer
    buffer.putData(DataPair(1, 10));
    assert(buffer.cur_size_ == 1);
    buffer.putData(DataPair(2, 20));
    assert(buffer.cur_size_ == 2);
    std::cout << "Buffer putData tests PASSED." << std::endl;

    // 3. test get data from buffer
    std::optional<DataPair> data_pair_option;
    data_pair_option = buffer.getData(1);
    assert(data_pair_option.has_value());
    assert(data_pair_option.value().key_ == 1);
    assert(data_pair_option.value().value_ == 10);
    data_pair_option = buffer.getData(2);
    assert(data_pair_option.has_value());
    assert(data_pair_option.value().key_ == 2);
    assert(data_pair_option.value().value_ == 20);
    data_pair_option = buffer.getData(3);
    assert(!data_pair_option.has_value());
    std::cout << "Buffer getData tests PASSED." << std::endl;

    // 4. test put same key in buffer
    assert(buffer.getData(1).value().value_ == 10);
    buffer.putData(DataPair(1, 100));
    assert(buffer.cur_size_ == 2);
    assert(buffer.getData(1).value().value_ == 100);
    std::cout << "Buffer put same key tests PASSED." << std::endl;
}


int main() {
    test_datapair();
    test_sstable();
    test_level();
    test_buffer();
    std::cout << "All tests passed!" << std::endl;
    return 0;
}
