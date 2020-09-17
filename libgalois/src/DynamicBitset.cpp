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

#include "galois/DynamicBitset.h"

#include "galois/Galois.h"

GALOIS_EXPORT galois::DynamicBitSet galois::EmptyBitset;

void
galois::DynamicBitSet::bitwise_or(const DynamicBitSet& other) {
  assert(size() == other.size());
  const auto& other_bitvec = other.get_vec();
  galois::do_all(
      galois::iterate(size_t{0}, bitvec.size()),
      [&](size_t i) { bitvec[i] |= other_bitvec[i]; }, galois::no_stats());
}

void
galois::DynamicBitSet::bitwise_and(const DynamicBitSet& other) {
  assert(size() == other.size());
  const auto& other_bitvec = other.get_vec();
  galois::do_all(
      galois::iterate(size_t{0}, bitvec.size()),
      [&](size_t i) { bitvec[i] &= other_bitvec[i]; }, galois::no_stats());
}

void
galois::DynamicBitSet::bitwise_and(
    const DynamicBitSet& other1, const DynamicBitSet& other2) {
  assert(size() == other1.size());
  assert(size() == other2.size());
  const auto& other_bitvec1 = other1.get_vec();
  const auto& other_bitvec2 = other2.get_vec();

  galois::do_all(
      galois::iterate(size_t{0}, bitvec.size()),
      [&](size_t i) { bitvec[i] = other_bitvec1[i] & other_bitvec2[i]; },
      galois::no_stats());
}

void
galois::DynamicBitSet::bitwise_xor(const DynamicBitSet& other) {
  assert(size() == other.size());
  const auto& other_bitvec = other.get_vec();
  galois::do_all(
      galois::iterate(size_t{0}, bitvec.size()),
      [&](size_t i) { bitvec[i] ^= other_bitvec[i]; }, galois::no_stats());
}

void
galois::DynamicBitSet::bitwise_xor(
    const DynamicBitSet& other1, const DynamicBitSet& other2) {
  assert(size() == other1.size());
  assert(size() == other2.size());
  const auto& other_bitvec1 = other1.get_vec();
  const auto& other_bitvec2 = other2.get_vec();

  galois::do_all(
      galois::iterate(size_t{0}, bitvec.size()),
      [&](size_t i) { bitvec[i] = other_bitvec1[i] ^ other_bitvec2[i]; },
      galois::no_stats());
}

uint64_t
galois::DynamicBitSet::count() const {
  galois::GAccumulator<uint64_t> ret;
  galois::do_all(
      galois::iterate(bitvec.begin(), bitvec.end()),
      [&](uint64_t n) {
#ifdef __GNUC__
        ret += __builtin_popcountll(n);
#else
        n = n - ((n >> 1) & 0x5555555555555555UL);
        n = (n & 0x3333333333333333UL) + ((n >> 2) & 0x3333333333333333UL);
        ret += (((n + (n >> 4)) & 0xF0F0F0F0F0F0F0FUL) * 0x101010101010101UL) >>
               56;
#endif
      },
      galois::no_stats());
  return ret.reduce();
}

std::vector<uint32_t>
galois::DynamicBitSet::getOffsets() const {
  // TODO uint32_t is somewhat dangerous; change in the future
  uint32_t activeThreads = galois::getActiveThreads();
  std::vector<unsigned int> tPrefixBitCounts(activeThreads);

  // count how many bits are set on each thread
  galois::on_each([&](unsigned tid, unsigned nthreads) {
    auto [start, end] =
        galois::block_range(size_t{0}, this->size(), tid, nthreads);

    unsigned int count = 0;
    for (unsigned int i = start; i < end; ++i) {
      if (this->test(i)) {
        ++count;
      }
    }

    tPrefixBitCounts[tid] = count;
  });

  // calculate prefix sum of bits per thread
  for (unsigned int i = 1; i < activeThreads; ++i) {
    tPrefixBitCounts[i] += tPrefixBitCounts[i - 1];
  }

  // total num of set bits
  uint64_t bitsetCount = tPrefixBitCounts[activeThreads - 1];
  std::vector<uint32_t> offsets;

  // calculate the indices of the set bits and save them to the offset
  // vector
  if (bitsetCount > 0) {
    offsets.resize(bitsetCount);
    galois::on_each([&](unsigned tid, unsigned nthreads) {
      auto [start, end] =
          galois::block_range(size_t{0}, this->size(), tid, nthreads);
      unsigned int count = 0;
      unsigned int tPrefixBitCount;
      if (tid == 0) {
        tPrefixBitCount = 0;
      } else {
        tPrefixBitCount = tPrefixBitCounts[tid - 1];
      }

      for (unsigned int i = start; i < end; ++i) {
        if (this->test(i)) {
          offsets[tPrefixBitCount + count] = i;
          ++count;
        }
      }
    });
  }

  return offsets;
}
