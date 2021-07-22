#pragma once

#include <cstddef>
#include <cstdlib>
#include <cstring>
#include <new>
#include <utility>

#include "katana/config.h"

namespace katana {

class KATANA_EXPORT HostHeap {
public:
  //! Allocate uninitialized items
  virtual void* Malloc(const size_t n_bytes) = 0;
  //! Allocate bitwise-zero-initialized items
  virtual void* Calloc(const size_t n_items, const size_t item_size) = 0;
  //! Reallocate the buffer of new size, copying the old data. O(N+M) space, where N is the old size and M is the new size.
  virtual void* Realloc(void* ptr, const size_t new_bytes) = 0;
  //! Release memory
  virtual void Free(void* ptr) = 0;
  //! Return the runtime flag whether allocations are fast (comparing to copying the memory) or not (e.g. because page locking is required)
  virtual bool IsFastAlloc() const = 0;

  virtual ~HostHeap();
};

class KATANA_EXPORT SwappableHostHeap : public HostHeap {
public:
  void* Malloc(const size_t n_bytes) override { return malloc(n_bytes); }

  void* Calloc(const size_t n_items, size_t item_size) override {
    return calloc(n_items, item_size);
  }

  void* Realloc(void* ptr, const size_t new_bytes) override {
    return realloc(ptr, new_bytes);
  }

  void Free(void* ptr) override { free(ptr); }

  bool IsFastAlloc() const override { return true; }

  ~SwappableHostHeap() override;
};

KATANA_EXPORT HostHeap* GetSwappableHostHeap();

template <typename Ty>
class HostAllocator {
  HostHeap* hh_;

  inline void destruct(char*) const {}
  inline void destruct(wchar_t*) const {}
  template <typename T>
  inline void destruct(T* t) const {
    t->~T();
  }

public:
  typedef size_t size_type;
  typedef ptrdiff_t difference_type;
  typedef Ty* pointer;
  typedef const Ty* const_pointer;
  typedef Ty& reference;
  typedef const Ty& const_reference;
  typedef Ty value_type;

  template <class Other>
  struct rebind {
    typedef HostAllocator<Other> other;
  };

  explicit HostAllocator(HostHeap* hh) noexcept : hh_(hh) {}

  template <class T1>
  HostAllocator(const HostAllocator<T1>& rhs) noexcept {
    hh_ = rhs.hh_;
  }

  inline pointer address(reference val) const { return &val; }

  inline const_pointer address(const_reference val) const { return &val; }

  pointer allocate(size_type size) {
    if (size > max_size())
      throw std::bad_alloc();
    return static_cast<pointer>(hh_->Malloc(size * sizeof(Ty)));
  }

  void deallocate(pointer ptr, size_type) { hh_->Free(ptr); }

  //! Allocate bitwise-zero-initialized items.
  Ty* Calloc(const size_t n_items) {
    return static_cast<Ty*>(hh_->Calloc(n_items, sizeof(Ty)));
  }

  //! Reallocate the buffer of new size, copying the old data. O(N+M) space, where N is the old size and M is the new size.
  //! If the new buffer is larger than the old one, then the tail items are uninitialized.
  Ty* Realloc(Ty* ptr, const size_t new_items) {
    return static_cast<Ty*>(hh_->Realloc(ptr, new_items * sizeof(Ty)));
  }

  //! Release the memory without requiring the allocation size.
  void Free(pointer ptr) { hh_->Free(ptr); }

  //! Return true if allocation is fast compared to copying the memory
  bool IsFastAlloc() const { return hh_->IsFastAlloc(); }

  inline void construct(pointer ptr, const_reference val) const {
    new (ptr) Ty(val);
  }

  template <class U, class... Args>
  inline void construct(U* p, Args&&... args) const {
    ::new ((void*)p) U(std::forward<Args>(args)...);
  }

  void destroy(pointer ptr) const { destruct(ptr); }

  size_type max_size() const noexcept { return size_t(-1) / sizeof(Ty); }

  template <typename T1>
  bool operator!=(const HostAllocator<T1>& rhs) const {
    return hh_ != rhs.hh_;
  }

  template <typename T1>
  bool operator==(const HostAllocator<T1>& rhs) const {
    return hh_ == rhs.hh_;
  }
};

}  // namespace katana
