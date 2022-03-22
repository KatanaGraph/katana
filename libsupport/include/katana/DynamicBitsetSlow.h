#ifndef KATANA_LIBSUPPORT_KATANA_DYNAMICBITSETSLOW_H_
#define KATANA_LIBSUPPORT_KATANA_DYNAMICBITSETSLOW_H_

#include <cassert>
#include <climits>
#include <vector>

#include <boost/iterator/counting_iterator.hpp>
#include <boost/mpl/has_xxx.hpp>

#include "katana/AtomicWrapper.h"
#include "katana/PODVector.h"
#include "katana/config.h"

namespace katana {

/// An iterator over dynamic bitsets.
///
/// Increment is an optimized linear search over the bitset, so iteration is
/// O(N) where N is the size of the bitset, not O(|S|) where |S| is the number
/// of set bits in the bit set.

// TODO(amp): This is a template so it can be used for both DynamicBitsetSlow
//  and DynamicBitset. This can be made a simple class once DynamicBitsetSlow
//  is removed.
template <typename DynamicBitsetType>
class KATANA_EXPORT DynamicBitsetIterator
    : public std::iterator<
          std::forward_iterator_tag, uint64_t, int64_t, const uint64_t*,
          uint64_t> {
  const DynamicBitsetType* underlying_;
  uint64_t array_index_;
  uint8_t bit_offset_;

public:
  DynamicBitsetIterator(
      const DynamicBitsetType* underlying, uint64_t array_index,
      uint8_t bit_offset)
      : underlying_(underlying),
        array_index_(array_index),
        bit_offset_(bit_offset) {}

  DynamicBitsetIterator& operator++() {
    // Step forward one to the bit we want to examine first.
    bit_offset_++;
    if (bit_offset_ > DynamicBitsetType::kNumBitsInUint64) {
      bit_offset_ = 0;
      array_index_++;
    }

    const auto& bitvec = underlying_->get_vec();
    const size_t size = underlying_->size();

    // Used only to make sure we stop on the last real used bit in cases where
    // the number of bits is not a multiple of kNumBitsInUint64.
    uint64_t current_bit_index = **this;

    // The following code is optimized to make the search process fast for
    // sparse bitsets. It's performance for densely filled bitsets should be
    // good too.

    // Iterate forward word by word
    for (; array_index_ < bitvec.size(); array_index_++) {
      uint64_t word = bitvec[array_index_].load(std::memory_order_relaxed);
      // For each word we check if it is non-zero (that is it contains a 1 bit)
      if (word != 0) {
        // Iterate over the bits in the work
        uint64_t bit_mask = uint64_t{1} << bit_offset_;
        for (; bit_offset_ < DynamicBitsetType::kNumBitsInUint64 &&
               current_bit_index < size;
             bit_offset_++, current_bit_index++, bit_mask <<= 1) {
          // Check if the bit is set. If it is we have reached where we need to be
          if ((word & bit_mask) != 0) {
            return *this;
          }
        }
        // Reset bit_offset_ here so that we start from our last bit_offset_ and
        // only reset when we roll over to the next word.
        bit_offset_ = 0;
      }
    }
    bit_offset_ = 0;
    array_index_ = bitvec.size();
    return *this;
  }

  DynamicBitsetIterator operator++(int) {
    auto r = *this;
    ++(*this);
    return r;
  }

  reference operator*() const {
    return array_index_ * DynamicBitsetType::kNumBitsInUint64 + bit_offset_;
  }

  bool operator==(const DynamicBitsetIterator& other) {
    return underlying_ == other.underlying_ &&
           array_index_ == other.array_index_ &&
           bit_offset_ == other.bit_offset_;
  }

  bool operator!=(const DynamicBitsetIterator& other) {
    return !(*this == other);
  }
};

//TODO(emcginnis): Remove this class entirely when DynamicBitset is available to libsupport
/**
 * Concurrent, thread safe, serial implementation of a dynamically allocated bitset
 * To be replaced with DynamicBitset once it is ripped out of libgalois
 **/
class KATANA_EXPORT DynamicBitsetSlow {
  katana::PODVector<katana::CopyableAtomic<uint64_t>> bitvec_;
  size_t num_bits_{0};

  /// DynamicBitsetSlow must maintain the invariant that the unused bits in the last
  /// element of bitvec_ are 0. This invariant is required for count() to return
  /// the correct value and also simplifies resizing bitsets to larger sizes.
  /// Most mutating methods maintain this invariant (for example, bitwise_and()
  /// will bitwise and the last entries of its operands together and if the
  /// inputs both have 0s in the unused bits the output will as well) but
  /// bitwise_not() must explicitly restore this invariant.
  void RestoreTrailingBitsInvariant() {
    if (size() > 0) {
      KATANA_LOG_DEBUG_ASSERT(!bitvec_.empty());

      size_t last_entry_offset =
          size() % (CHAR_BIT * sizeof(decltype(bitvec_)::value_type));
      if (last_entry_offset != 0) {
        uint64_t last_entry_mask = (1UL << last_entry_offset) - 1;
        bitvec_.back() = bitvec_.back() & last_entry_mask;
      }
    }
  }

public:
  using iterator = DynamicBitsetIterator<DynamicBitsetSlow>;

  static constexpr uint32_t kNumBitsInUint64 = sizeof(uint64_t) * CHAR_BIT;

  explicit DynamicBitsetSlow(
      const HostAllocator<katana::CopyableAtomic<uint64_t>>& host_alloc = {})
      : bitvec_(host_alloc){};

  DynamicBitsetSlow(DynamicBitsetSlow&& bitset)
      : bitvec_(std::move(bitset.bitvec_)), num_bits_(bitset.num_bits_) {
    bitset.num_bits_ = 0;
  }

  DynamicBitsetSlow(const DynamicBitsetSlow& bitset)
      : num_bits_(bitset.num_bits_) {
    bitvec_.resize(bitset.bitvec_.size());
    std::copy(bitset.bitvec_.begin(), bitset.bitvec_.end(), bitvec_.begin());
  }

  DynamicBitsetSlow& operator=(DynamicBitsetSlow&& bitset) {
    if (this != &bitset) {
      bitvec_ = std::move(bitset.bitvec_);
      num_bits_ = bitset.num_bits_;
      bitset.num_bits_ = 0;
    }
    return *this;
  }

  DynamicBitsetSlow& operator=(const DynamicBitsetSlow& bitset) {
    if (this != &bitset) {
      num_bits_ = bitset.num_bits_;
      bitvec_.resize(bitset.bitvec_.size());
      std::copy(bitset.bitvec_.begin(), bitset.bitvec_.end(), bitvec_.begin());
    }
    return *this;
  }

  /**
   * Returns the underlying bitset representation to the user
   *
   * @returns constant reference vector of copyable atomics that represents
   * the bitset
   */
  const auto& get_vec() const { return bitvec_; }

  /**
   * Returns the underlying bitset representation to the user
   *
   * @returns reference to vector of copyable atomics that represents the
   * bitset
   */
  auto& get_vec() { return bitvec_; }

  iterator begin() const {
    iterator bit0{this, 0, 0};
    if (test(0)) {
      // If bit 0 is set then we have the right iterator
      return bit0;
    }
    // Otherwise, increment to find the first set bit.
    return ++bit0;
  }

  iterator end() const { return {this, bitvec_.size(), 0}; }

  /**
   * Resizes the bitset.
   *
   * @param n Size to change the bitset to
   */
  void resize(size_t n) {
    KATANA_LOG_DEBUG_ASSERT(
        kNumBitsInUint64 == 64);  // compatibility with other devices
    num_bits_ = n;
    size_t old_size = bitvec_.size();
    bitvec_.resize((n + kNumBitsInUint64 - 1) / kNumBitsInUint64);
    if (bitvec_.size() > old_size) {
      std::fill(bitvec_.begin() + old_size, bitvec_.end(), 0);
    }
  }

  /**
   * Reserves capacity for the bitset.
   *
   * @param n Size to reserve the capacity of the bitset to
   */
  void reserve(size_t n) {
    KATANA_LOG_DEBUG_ASSERT(
        kNumBitsInUint64 == 64);  // compatibility with other devices
    bitvec_.reserve((n + kNumBitsInUint64 - 1) / kNumBitsInUint64);
  }

  /**
   * Clears the bitset.
   */
  void clear() {
    num_bits_ = 0;
    // TODO(roshan) parallelize this.
    std::fill(bitvec_.begin(), bitvec_.end(), 0);
    bitvec_.clear();
  }

  /**
   * Shrinks the allocation for bitset to its current size.
   */
  void shrink_to_fit() { bitvec_.shrink_to_fit(); }

  /**
   * Gets the size of the bitset
   * @returns The number of bits held by the bitset
   */
  size_t size() const { return num_bits_; }

  /**
   * Unset every bit in the bitset.
   */
  void reset() { std::fill(bitvec_.begin(), bitvec_.end(), 0); }

  /**
   * Unset a range of bits given an inclusive range
   *
   * @param begin first bit in range to reset
   * @param end last bit in range to reset
   */
  void reset(size_t begin, size_t end) {
    if (num_bits_ == 0) {
      return;
    }

    KATANA_LOG_DEBUG_ASSERT(begin <= (num_bits_ - 1));
    KATANA_LOG_DEBUG_ASSERT(end <= (num_bits_ - 1));

    // 100% safe implementation, but slow
    // for (unsigned long i = begin; i <= end; i++) {
    //  size_t bit_index = i / kNumBitsInUint64;
    //  uint64_t bit_offset = 1;
    //  bit_offset <<= (i % kNumBitsInUint64);
    //  uint64_t mask = ~bit_offset;
    //  bitvec_[bit_index] &= mask;
    //}

    // block which you are safe to clear
    size_t vec_begin = (begin + kNumBitsInUint64 - 1) / kNumBitsInUint64;
    size_t vec_end;

    if (end == (num_bits_ - 1))
      vec_end = bitvec_.size();
    else
      vec_end = (end + 1) / kNumBitsInUint64;  // floor

    if (vec_begin < vec_end) {
      std::fill(bitvec_.begin() + vec_begin, bitvec_.begin() + vec_end, 0);
    }

    vec_begin *= kNumBitsInUint64;
    vec_end *= kNumBitsInUint64;

    // at this point vec_begin -> vec_end-1 has been reset

    if (vec_begin > vec_end) {
      // no fill happened
      if (begin < vec_begin) {
        size_t diff = vec_begin - begin;
        KATANA_LOG_DEBUG_ASSERT(diff < 64);
        uint64_t mask = (uint64_t{1} << (64 - diff)) - 1;

        size_t end_diff = end - vec_end + 1;
        uint64_t or_mask = (uint64_t{1} << end_diff) - 1;
        mask |= ~or_mask;

        size_t bit_index = begin / kNumBitsInUint64;
        bitvec_[bit_index] &= mask;
      }
    } else {
      if (begin < vec_begin) {
        size_t diff = vec_begin - begin;
        KATANA_LOG_DEBUG_ASSERT(diff < 64);
        uint64_t mask = (uint64_t{1} << (64 - diff)) - 1;
        size_t bit_index = begin / kNumBitsInUint64;
        bitvec_[bit_index] &= mask;
      }
      if (end >= vec_end) {
        size_t diff = end - vec_end + 1;
        KATANA_LOG_DEBUG_ASSERT(diff < 64);
        uint64_t mask = (uint64_t{1} << diff) - 1;
        size_t bit_index = end / kNumBitsInUint64;
        bitvec_[bit_index] &= ~mask;
      }
    }
  }

  /**
   * Check a bit to see if it is currently set.
   * Using this is recommended only if set() and reset()
   * are not being used in that parallel section/phase
   *
   * @param index Bit to check to see if set
   * @returns true if index is set
   */
  bool test(size_t index) const {
    KATANA_LOG_DEBUG_ASSERT(index < num_bits_);
    size_t bit_index = index / kNumBitsInUint64;
    uint64_t bit_offset = 1;
    bit_offset <<= (index % kNumBitsInUint64);
    return (
        (bitvec_[bit_index].load(std::memory_order_relaxed) & bit_offset) != 0);
  }

  /**
   * Set a bit in the bitset.
   *
   * @param index Bit to set
   * @returns the old value
   */
  bool set(size_t index) {
    size_t bit_index = index / kNumBitsInUint64;
    uint64_t bit_offset = 1;
    bit_offset <<= (index % kNumBitsInUint64);
    uint64_t old_val = bitvec_[bit_index];
    // test and set
    // if old_bit is 0, then atomically set it
    while (((old_val & bit_offset) == 0) &&
           !bitvec_[bit_index].compare_exchange_weak(
               old_val, old_val | bit_offset, std::memory_order_relaxed)) {
      ;
    }
    return (old_val & bit_offset);
  }

  void set() {
    for (size_t i = 0; i < size(); i++) {
      set(i);
    }
  }

  /**
   * Reset a bit in the bitset.
   *
   * @param index Bit to reset
   * @returns the old value
   */
  bool reset(size_t index) {
    size_t bit_index = index / kNumBitsInUint64;
    uint64_t bit_offset = 1;
    bit_offset <<= (index % kNumBitsInUint64);
    uint64_t old_val = bitvec_[bit_index];
    // test and reset
    // if old_bit is 1, then atomically reset it
    while (((old_val & bit_offset) != 0) &&
           !bitvec_[bit_index].compare_exchange_weak(
               old_val, old_val & ~bit_offset, std::memory_order_relaxed)) {
      ;
    }
    return (old_val & bit_offset);
  }

  // assumes bit_vector is not updated (set) in parallel
  void bitwise_or(const DynamicBitsetSlow& other);

  /**
   * Does an IN-PLACE bitwise or of 2 passed in bitsets and saves to this
   * bitset
   *
   * @param other1 Bitset to and with other 2
   * @param other2 Bitset to and with other 1
   */
  void bitwise_or(
      const DynamicBitsetSlow& other1, const DynamicBitsetSlow& other2);

  // assumes bit_vector is not updated (set) in parallel
  void bitwise_not();

  // assumes bit_vector is not updated (set) in parallel

  /**
   * Does an IN-PLACE bitwise and of this bitset and another bitset
   *
   * @param other Other bitset to do bitwise and with
   */
  void bitwise_and(const DynamicBitsetSlow& other);

  /**
   * Does an IN-PLACE bitwise and of 2 passed in bitsets and saves to this
   * bitset
   *
   * @param other1 Bitset to and with other 2
   * @param other2 Bitset to and with other 1
   */
  void bitwise_and(
      const DynamicBitsetSlow& other1, const DynamicBitsetSlow& other2);

  /**
   * Does an IN-PLACE bitwise xor of this bitset and another bitset
   *
   * @param other Other bitset to do bitwise xor with
   */
  void bitwise_xor(const DynamicBitsetSlow& other);

  /**
   * Does an IN-PLACE bitwise and of 2 passed in bitsets and saves to this
   * bitset
   *
   * @param other1 Bitset to xor with other 2
   * @param other2 Bitset to xor with other 1
   */
  void bitwise_xor(
      const DynamicBitsetSlow& other1, const DynamicBitsetSlow& other2);

  bool all();

  DynamicBitsetSlow& operator|=(const DynamicBitsetSlow& other) {
    KATANA_LOG_ASSERT(size() == other.size());
    bitwise_or(other);
    return *this;
  }

  DynamicBitsetSlow& operator&=(const DynamicBitsetSlow& other) {
    KATANA_LOG_ASSERT(size() == other.size());
    bitwise_and(other);
    return *this;
  }

  bool operator==(const DynamicBitsetSlow& other) const {
    return Equals(other);
  }

  bool operator!=(const DynamicBitsetSlow& other) const {
    return !Equals(other);
  }

  bool Equals(const DynamicBitsetSlow& other) const {
    if (size() != other.size()) {
      return false;
    }
    for (size_t i = 0; i < size(); i++) {
      if (test(i) != other.test(i)) {
        return false;
      }
    }
    return true;
  }

  //TODO(emcginnis): DynamicBitsetSlow is not actually memory copyable, remove this
  //! this is defined to
  using tt_is_copyable = int;
};

}  // namespace katana
#endif
