#ifndef BLOOM_FILTER_HH
#define BLOOM_FILTER_HH

#include <vector>
#include <string>
#include <functional>
#include <cmath>
#include <algorithm>

const double DEFAULT_FALSE_POSITIVE_RATE = 0.01;

class BloomFilter {
    public:
    size_t num_bits_;
    size_t num_hashes_;
    double fp_rate_;

    // set a hash prime; apparently this one is called the golden ratio
    static const size_t hash_prime_ = 0x9e3779b9;

    // bits packed into bytes
    std::vector<unsigned char> bits_;
    // bit manipulation
    void set_bit(size_t bit_index);
    bool get_bit(size_t bit_index) const;

    // constructor
    BloomFilter(size_t items_num, double fp_rate = DEFAULT_FALSE_POSITIVE_RATE);
    BloomFilter(size_t num_bits_val, size_t num_hashes_val,
                const std::vector<unsigned char>& bit_storage_val, 
                double original_fp_rate = DEFAULT_FALSE_POSITIVE_RATE);

    void add(long key);
    bool might_contain(long key) const;

    // generate k hashes for an input key
    std::vector<size_t> generate_k_hashes(long key) const;
};

#endif

