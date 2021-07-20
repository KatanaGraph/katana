#pragma once

#include <cstdlib>
#include <cstring>

#include "katana/config.h"

namespace katana {

//TODO (serge): change to a polymorphic allocator to switch between pinned and swappable memory
enum class MemoryPinType { Swappable = 0, Pinned = 1 };

#ifdef KATANA_ENABLE_GPU
static constexpr const bool kGpuEnabled = true;
static constexpr const MemoryPinType kUseMemoryPinType = MemoryPinType::Pinned;
#else
static constexpr const bool kGpuEnabled = false;
static constexpr const MemoryPinType kUseMemoryPinType =
    MemoryPinType::Swappable;
#endif  // KATANA_ENABLE_GPU

class BaseHostAllocator {
public:
  virtual void* Malloc(const size_t n_bytes) const = 0;
  virtual void* Calloc(const size_t n_items, const size_t item_size) const = 0;
  virtual void* Realloc(void* ptr, const size_t n_bytes) const = 0;
  virtual void Free(void* ptr) const = 0;
  virtual MemoryPinType GetType() const = 0;
  virtual ~BaseHostAllocator() {}
};

class SwappableHostAllocator : public BaseHostAllocator {
public:
  void* Malloc(const size_t n_bytes) const override { return malloc(n_bytes); }
  void* Calloc(const size_t n_items, const size_t item_size) const override {
    return calloc(n_items, item_size);
  }
  void* Realloc(void* ptr, const size_t n_bytes) const override {
    return realloc(ptr, n_bytes);
  }
  void Free(void* ptr) const override { free(ptr); }
  MemoryPinType GetType() const override { return MemoryPinType::Swappable; }
};

KATANA_EXPORT extern const SwappableHostAllocator swappable_host_allocator;

}  // namespace katana
