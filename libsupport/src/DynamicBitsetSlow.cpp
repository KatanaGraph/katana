#include "katana/DynamicBitsetSlow.h"

#include <algorithm>

void
katana::DynamicBitsetSlow::bitwise_or(const DynamicBitsetSlow& other) {
  KATANA_LOG_DEBUG_ASSERT(size() == other.size());
  const auto& other_bitvec = other.get_vec();

  for (size_t i = 0; i < bitvec_.size(); i++) {
    bitvec_[i] |= other_bitvec[i];
  }
}

void
katana::DynamicBitsetSlow::bitwise_or(
    const DynamicBitsetSlow& other1, const DynamicBitsetSlow& other2) {
  KATANA_LOG_DEBUG_ASSERT(size() == other1.size());
  KATANA_LOG_DEBUG_ASSERT(size() == other2.size());
  const auto& other_bitvec1 = other1.get_vec();
  const auto& other_bitvec2 = other2.get_vec();

  for (size_t i = 0; i < bitvec_.size(); i++) {
    bitvec_[i] = other_bitvec1[i] | other_bitvec2[i];
  }
}

void
katana::DynamicBitsetSlow::bitwise_not() {
  for (size_t i = 0; i < bitvec_.size(); i++) {
    bitvec_[i] = ~bitvec_[i];
  }
}

void
katana::DynamicBitsetSlow::bitwise_and(const DynamicBitsetSlow& other) {
  KATANA_LOG_DEBUG_ASSERT(size() == other.size());
  const auto& other_bitvec = other.get_vec();
  for (size_t i = 0; i < bitvec_.size(); i++) {
    bitvec_[i] &= other_bitvec[i];
  }
}

void
katana::DynamicBitsetSlow::bitwise_and(
    const DynamicBitsetSlow& other1, const DynamicBitsetSlow& other2) {
  KATANA_LOG_DEBUG_ASSERT(size() == other1.size());
  KATANA_LOG_DEBUG_ASSERT(size() == other2.size());
  const auto& other_bitvec1 = other1.get_vec();
  const auto& other_bitvec2 = other2.get_vec();

  for (size_t i = 0; i < bitvec_.size(); i++) {
    bitvec_[i] = other_bitvec1[i] & other_bitvec2[i];
  }
}

void
katana::DynamicBitsetSlow::bitwise_xor(const DynamicBitsetSlow& other) {
  KATANA_LOG_DEBUG_ASSERT(size() == other.size());
  const auto& other_bitvec = other.get_vec();

  for (size_t i = 0; i < bitvec_.size(); i++) {
    bitvec_[i] ^= other_bitvec[i];
  }
}

void
katana::DynamicBitsetSlow::bitwise_xor(
    const DynamicBitsetSlow& other1, const DynamicBitsetSlow& other2) {
  KATANA_LOG_DEBUG_ASSERT(size() == other1.size());
  KATANA_LOG_DEBUG_ASSERT(size() == other2.size());
  const auto& other_bitvec1 = other1.get_vec();
  const auto& other_bitvec2 = other2.get_vec();

  for (size_t i = 0; i < bitvec_.size(); i++) {
    bitvec_[i] = other_bitvec1[i] ^ other_bitvec2[i];
  }
}

bool
katana::DynamicBitsetSlow::all() {
  for (size_t i = 0; i < size(); i++) {
    if (!test(i)) {
      return false;
    }
  }
  return true;
}
