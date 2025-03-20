#include "lsm_tree.hh"
#include <iostream>
#include <vector>
#include <cassert>
#include <string>
#include <memory>

void test_sstable();
// SSTable tests
void test_sstable() {
    std::cout << "[TEST] Testing SSTable ---" << std::endl;

    // 1. test empty SSTable
    std::vector<DataPair> empty_data;
    SSTable empty_table(empty_data, 1);
    assert(empty_table.size_ == 0);
    assert(empty_table.min_key_ == std::numeric_limits<long>::max());
    assert(empty_table.max_key_ == std::numeric_limits<long>::min());
    assert(!empty_table.keyInRange(10));
    assert(!empty_table.getDataPair(10).has_value());
    std::cout << "Empty SSTable tests PASSED." << std::endl;
}

int main() {
    test_sstable();
    std::cout << "All tests passed!" << std::endl;

    return 0;
}
