/*
 * This file belongs to the Galois project, a C++ library for exploiting
 * parallelism. The code is being released under the terms of the 3-Clause BSD
 * License (a copy is located in LICENSE.txt at the top-level directory).
 *
 * Copyright (C) 2019, The University of Texas at Austin. All rights reserved.
 * UNIVERSITY EXPRESSLY DISCLAIMS ANY AND ALL WARRANTIES CONCERNING THIS
 * SOFTWARE AND DOCUMENTATION, INCLUDING ANY WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR ANY PARTICULAR PURPOSE, NON-INFRINGEMENT AND WARRANTIES OF
 * PERFORMANCE, AND ANY WARRANTY THAT MIGHT OTHERWISE ARISE FROM COURSE OF
 * DEALING OR USAGE OF TRADE.  NO WARRANTY IS EITHER EXPRESS OR IMPLIED WITH
 * RESPECT TO THE USE OF THE SOFTWARE OR DOCUMENTATION. Under no circumstances
 * shall University be liable for incidental, special, indirect, direct or
 * consequential damages or loss of profits, interruption of business, or
 * related expenses which may arise from use of Software or Documentation,
 * including but not limited to those resulting from defects in Software and/or
 * Documentation, or loss or inaccuracy of data of any kind.
 */

/**
 * @file DynamicBitset.cpp
 */

#include "katana/DynamicBitset.h"

#include "katana/Galois.h"

KATANA_EXPORT katana::DynamicBitset katana::EmptyBitset;

namespace {
// This namespace contains bit tricks

/// Counts the number of set bits in a uint64.
inline uint64_t
CountSetBits(uint64_t int_to_count) {
#ifdef __GNUC__
  return __builtin_popcountll(int_to_count);
#else
  int_to_count = int_to_count - ((int_to_count >> 1) & 0x5555555555555555UL);
  int_to_count = (int_to_count & 0x3333333333333333UL) +
                 ((int_to_count >> 2) & 0x3333333333333333UL);
  return (((int_to_count + (int_to_count >> 4)) & 0xF0F0F0F0F0F0F0FUL) *
          0x101010101010101UL) >>
         56;
#endif
}

/// Counts trailing 0s in an int
inline uint64_t
CountTrailingZeroes(uint64_t int_to_count) {
#ifdef __GNUC__
  return __builtin_ctzll(int_to_count);
#else
  // TODO(l-hoang) replace with something more concrete and don't make compiler
  // optimize (if it optimizes this at all)?
  uint64_t mask = uint64_t{1};
  uint8_t current_trailing_zeros = 0;

  while (current_trailing_zeros < 64) {
    if ((mask & int_to_count) == mask) {
      return current_trailing_zeros;
    }
    mask <<= 1;
    current_trailing_zeros++;
  }

  KATANA_LOG_FATAL(
      "dev error: this function shouldn't have been called with 0");
  return -1;
#endif
}

}  // namespace

void
katana::DynamicBitset::bitwise_or(const DynamicBitset& other) {
  KATANA_LOG_DEBUG_ASSERT(size() == other.size());
  const auto& other_bitvec = other.get_vec();
  katana::do_all(
      katana::iterate(size_t{0}, bitvec_.size()),
      [&](size_t i) { bitvec_[i] |= other_bitvec[i]; }, katana::no_stats());
}

void
katana::DynamicBitset::bitwise_not() {
  katana::do_all(
      katana::iterate(size_t{0}, bitvec_.size()),
      [&](size_t i) { bitvec_[i] = ~bitvec_[i]; }, katana::no_stats());

  RestoreTrailingBitsInvariant();
}

void
katana::DynamicBitset::bitwise_and(const DynamicBitset& other) {
  KATANA_LOG_DEBUG_ASSERT(size() == other.size());
  const auto& other_bitvec = other.get_vec();
  katana::do_all(
      katana::iterate(size_t{0}, bitvec_.size()),
      [&](size_t i) { bitvec_[i] &= other_bitvec[i]; }, katana::no_stats());
}

void
katana::DynamicBitset::bitwise_and(
    const DynamicBitset& other1, const DynamicBitset& other2) {
  KATANA_LOG_DEBUG_ASSERT(size() == other1.size());
  KATANA_LOG_DEBUG_ASSERT(size() == other2.size());
  const auto& other_bitvec1 = other1.get_vec();
  const auto& other_bitvec2 = other2.get_vec();

  katana::do_all(
      katana::iterate(size_t{0}, bitvec_.size()),
      [&](size_t i) { bitvec_[i] = other_bitvec1[i] & other_bitvec2[i]; },
      katana::no_stats());
}

void
katana::DynamicBitset::bitwise_xor(const DynamicBitset& other) {
  KATANA_LOG_DEBUG_ASSERT(size() == other.size());
  const auto& other_bitvec = other.get_vec();
  katana::do_all(
      katana::iterate(size_t{0}, bitvec_.size()),
      [&](size_t i) { bitvec_[i] ^= other_bitvec[i]; }, katana::no_stats());
}

void
katana::DynamicBitset::bitwise_xor(
    const DynamicBitset& other1, const DynamicBitset& other2) {
  KATANA_LOG_DEBUG_ASSERT(size() == other1.size());
  KATANA_LOG_DEBUG_ASSERT(size() == other2.size());
  const auto& other_bitvec1 = other1.get_vec();
  const auto& other_bitvec2 = other2.get_vec();

  katana::do_all(
      katana::iterate(size_t{0}, bitvec_.size()),
      [&](size_t i) { bitvec_[i] = other_bitvec1[i] ^ other_bitvec2[i]; },
      katana::no_stats());
}

size_t
katana::DynamicBitset::count() const {
  katana::GAccumulator<size_t> ret;
  katana::do_all(
      katana::iterate(bitvec_.begin(), bitvec_.end()),
      [&](uint64_t n) { ret += CountSetBits(n); }, katana::no_stats());
  return ret.reduce();
}

size_t
katana::DynamicBitset::SerialCount() const {
  size_t ret = 0;
  for (uint64_t n : bitvec_) {
    ret += CountSetBits(n);
  }
  return ret;
}

namespace {

template <typename Integer>
void
ComputeOffsets(
    const katana::DynamicBitset& bitset, std::vector<Integer>* set_elements) {
  uint32_t active_threads = katana::getActiveThreads();
  std::vector<Integer> thread_prefix_bit_counts(active_threads);

  const katana::PODVector<katana::DynamicBitset::TItem>& underlying_bitvec =
      bitset.get_vec();

  // count how many bits are set on each thread
  katana::on_each([&](unsigned tid, unsigned nthreads) {
    auto [start, end] =
        katana::block_range(size_t{0}, underlying_bitvec.size(), tid, nthreads);
    Integer count = 0;
    for (uint64_t bitvec_index = start; bitvec_index < end; ++bitvec_index) {
      count += CountSetBits(underlying_bitvec[bitvec_index]);
    }
    thread_prefix_bit_counts[tid] = count;
  });

  // calculate prefix sum of bits per thread
  for (uint32_t i = 1; i < active_threads; ++i) {
    thread_prefix_bit_counts[i] += thread_prefix_bit_counts[i - 1];
  }
  // total num of set bits
  Integer bitset_count = thread_prefix_bit_counts[active_threads - 1];

  // calculate the indices of the set bits and save them to the offset
  // vector
  if (bitset_count > 0) {
    size_t cur_size = set_elements->size();
    set_elements->resize(cur_size + bitset_count);

    katana::on_each([&](unsigned tid, unsigned nthreads) {
      auto [start, end] = katana::block_range(
          size_t{0}, underlying_bitvec.size(), tid, nthreads);

      Integer index = cur_size;
      if (tid != 0) {
        index += thread_prefix_bit_counts[tid - 1];
      }

      for (uint64_t bitvec_index = start; bitvec_index < end; ++bitvec_index) {
        // get set bits and add
        uint64_t current_num = underlying_bitvec[bitvec_index];
        uint64_t offset =
            bitvec_index * katana::DynamicBitset::kNumBitsInUint64;

        if (current_num == 0) {
          // nothing to add
          continue;
        }

        if (current_num == 1) {
          // trailing 0s indicate location of set bit
          set_elements->at(index++) = offset + CountTrailingZeroes(current_num);
        } else if (current_num == std::numeric_limits<uint64_t>::max()) {
          // add all
          for (size_t i = 0; i < katana::DynamicBitset::kNumBitsInUint64; i++) {
            set_elements->at(index++) = offset + i;
          }
        } else {
          // add only set parts
          for (size_t i = 0; i < katana::DynamicBitset::kNumBitsInUint64; i++) {
            if (bitset.test(offset + i)) {
              set_elements->at(index++) = offset + i;
            }
          }
        }
      }
    });
  }
}

template <typename Integer>
void
ComputeOffsetsSerial(
    const katana::DynamicBitset& bitset, std::vector<Integer>* set_elements) {
  const katana::PODVector<katana::DynamicBitset::TItem>& underlying_bitvec =
      bitset.get_vec();
  set_elements->reserve(bitset.SerialCount());

  // loop through each int of the bitset invidiually
  for (size_t bit_index = 0; bit_index < underlying_bitvec.size();
       bit_index++) {
    uint64_t int_to_examine = underlying_bitvec[bit_index];

    if (int_to_examine == 0) {
      // nothing set, skip
      continue;
    }

    uint64_t offset = bit_index * katana::DynamicBitset::kNumBitsInUint64;

    // TODO(l-hoang) optimize 63 set bits case? (find only 0, add the rest)
    // optimize special corner cases case
    if (int_to_examine == 1) {
      // find trailing zeros gets you the set bit without needing to loop over
      // all 64 bits
      set_elements->emplace_back(offset + CountTrailingZeroes(int_to_examine));
    } else if (int_to_examine == std::numeric_limits<uint64_t>::max()) {
      // all set: add all, no testing required
      for (size_t i = 0; i < katana::DynamicBitset::kNumBitsInUint64; i++) {
        set_elements->emplace_back(offset + i);
      }
    } else {
      // loop through all; hope is that this is rare
      // TODO(l-hoang) is there a better way to do this, i.e. is test required?
      for (size_t i = 0; i < katana::DynamicBitset::kNumBitsInUint64; i++) {
        if (bitset.test(offset + i)) {
          set_elements->emplace_back(offset + i);
        }
      }
    }
  }
}

}  // namespace

template <>
std::vector<uint32_t>
katana::DynamicBitset::GetOffsets<uint32_t>() const {
  std::vector<uint32_t> offsets;
  ComputeOffsets<uint32_t>(*this, &offsets);
  return offsets;
}

template <>
std::vector<uint64_t>
katana::DynamicBitset::GetOffsets<uint64_t>() const {
  std::vector<uint64_t> offsets;
  ComputeOffsets<uint64_t>(*this, &offsets);
  return offsets;
}

template <>
std::vector<uint32_t>
katana::DynamicBitset::GetOffsetsSerial<uint32_t>() const {
  std::vector<uint32_t> offsets;
  ComputeOffsetsSerial<uint32_t>(*this, &offsets);
  return offsets;
}

template <>
std::vector<uint64_t>
katana::DynamicBitset::GetOffsetsSerial<uint64_t>() const {
  std::vector<uint64_t> offsets;
  ComputeOffsetsSerial<uint64_t>(*this, &offsets);
  return offsets;
}

template <>
void
katana::DynamicBitset::AppendOffsets(std::vector<uint32_t>* offsets) const {
  ComputeOffsets<uint32_t>(*this, offsets);
}

template <>
void
katana::DynamicBitset::AppendOffsets(std::vector<uint64_t>* offsets) const {
  ComputeOffsets<uint64_t>(*this, offsets);
}
