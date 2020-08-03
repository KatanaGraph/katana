#ifndef GALOIS_LIBTSUBA_TSUBAINTERNAL_H_
#define GALOIS_LIBTSUBA_TSUBAINTERNAL_H_

#include <cerrno>
#include <thread>
#include <cstdint>
#include <cassert>

constexpr const uint64_t kKBShift = 10;
constexpr const uint64_t kMBShift = 20;
constexpr const uint64_t kGBShift = 30;

template <typename T>
constexpr T KB(T v) {
  return (uint64_t)v << kKBShift;
}
template <typename T>
constexpr T MB(T v) {
  return (uint64_t)v << kMBShift;
}
template <typename T>
constexpr T GB(T v) {
  return (uint64_t)v << kGBShift;
}

#endif
