#include "bloom_filter.hh"

#include <iostream>

int main() {
    size_t items_num = 10;
    float fp_rate = 0.3;
    BloomFilter bloom_filter(items_num, fp_rate);

    // Add some keys to the filter
    for (long i = 1; i <= 10; ++i) {
        bloom_filter.add(i);
    }

    // Check if keys are in the filter
    for (long i = 1; i <= 20; ++i) {
        if (bloom_filter.might_contain(i)) {
            std::cout << "Key " << i << " might be in the filter." << std::endl;
        } else {
            std::cout << "Key " << i << " is definitely not in the filter." << std::endl;
        }
    }

    return 0;
}