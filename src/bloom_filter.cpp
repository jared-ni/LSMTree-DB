#include "bloom_filter.hh"
#include <iostream>

// constructor for Bloom filter
// number of bits = capacity * -ln(error_rate) / ln(2)^2
// number of hashes = k = (bit_size / expected_num_of_elements) * ln(2)
BloomFilter::BloomFilter(size_t items_num, double fp_rate) {
    this->fp_rate_ = fp_rate;

    // std::cout << "[BloomFilter] items_num: " << items_num 
    //           << ", fp_rate: " << this->fp_rate_ << std::endl;

    double m_double = std::ceil(
        -(static_cast<double>(items_num) * std::log(fp_rate)) / (std::log(2.) * std::log(2.))
    );

    this->num_bits_ = std::max(m_double, 1.0);

    double k_double = std::round(
        (static_cast<double>(num_bits_) / static_cast<double>(items_num)) * std::log(2.)
    );

    this->num_hashes_ = std::max(k_double, 1.);

    // assign num_bits to bits_, packed in bytes, and set them all to false
    if (num_bits_ > 0) {
        size_t num_bytes = (num_bits_ + CHAR_BIT - 1) / CHAR_BIT;
        this->bits_.assign(num_bytes, 0);
    }
}

// Constructor for loading from raw byte storage
BloomFilter::BloomFilter(size_t num_bits_val, size_t num_hashes_val,
                         const std::vector<unsigned char>& bit_storage_val, double original_fp_rate) {
    this->num_bits_ = num_bits_val;
    this->num_hashes_ = num_hashes_val;
    this->fp_rate_ = original_fp_rate;
    this->bits_ = bit_storage_val;                   

    size_t expected_bytes = (num_bits_ + CHAR_BIT - 1) / CHAR_BIT;

    if (bits_.size() != expected_bytes && num_bits_ > 0) {
        std::cerr << "Error: BloomFilter loaded with mismatched bit_storage size." << std::endl;
        this->num_bits_ = 0;
        this->num_hashes_ = 0;
        this->bits_.clear();
    }
}

void BloomFilter::set_bit(size_t bit_index) {
    if (bit_index >= num_bits_) {
        std::cerr << "Error: bit_index out of range in set_bit." << std::endl;
        return;
    }
    size_t byte_index = bit_index / CHAR_BIT;
    unsigned char bit_mask = (1 << (bit_index % CHAR_BIT));
    // use bitwise OR to assign the bit to true
    bits_[byte_index] = bits_[byte_index] | bit_mask;
}

// gets the value of a bit at the given index
bool BloomFilter::get_bit(size_t bit_index) const {
    if (bit_index >= num_bits_) {
        std::cerr << "Error: bit_index out of range in get_bit." << std::endl;
        return false;
    }
    size_t byte_index = bit_index / CHAR_BIT;
    unsigned char bit_mask = (1 << (bit_index % CHAR_BIT));

    return (bits_[byte_index] & bit_mask) != 0;
}


// for every value, we can generate the k hashes for it
// must be consistent every time
std::vector<size_t> BloomFilter::generate_k_hashes(long key) const {
    std::vector<size_t> hashes(num_hashes_);
    
    std::hash<long> hasher;
    size_t hash1 = hasher(key);
    size_t hash2 = hasher(hash1 ^ hash_prime_);

    for (size_t i = 0; i < num_hashes_; ++i) {
        hashes[i] = (hash1 + i * hash2) % num_bits_;
    }
    return hashes;
}

// add a key to bloom filter, by setting its bits to true
void BloomFilter::add(long key) {
    std::vector<size_t> hash_vals = generate_k_hashes(key);
    for (size_t hv : hash_vals) {
        // bits_[hv] = true;
        set_bit(hv);
    }
}

// if any of the bits is false, then the key is definitely not in the filter
bool BloomFilter::might_contain(long key) const {
    std::vector<size_t> hash_vals = generate_k_hashes(key);
    for (size_t hv : hash_vals) {
        if (!get_bit(hv)) {
            return false;
        }
    }
    return true;
}