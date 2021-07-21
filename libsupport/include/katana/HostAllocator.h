#pragma once

#include <cstdlib>
#include <cstring>

#include "katana/config.h"

namespace katana {

enum class MemoryPinType { Swappable = 0, Pinned = 1 };

class KATANA_EXPORT HostAllocator {
public:
  virtual void* Malloc(const size_t n_bytes) const = 0;
  virtual void* Calloc(const size_t n_items, const size_t item_size) const = 0;
  virtual void* Realloc(void* ptr, const size_t n_bytes) const = 0;
  virtual void Free(void* ptr) const = 0;
  virtual MemoryPinType GetType() const = 0;
  virtual ~HostAllocator();
};

class KATANA_EXPORT SwappableHostAllocator : public HostAllocator {
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
  ~SwappableHostAllocator() override;
};

KATANA_EXPORT extern const SwappableHostAllocator swappable_host_allocator;

}  // namespace katana
