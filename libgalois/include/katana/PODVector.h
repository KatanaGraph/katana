/*
 * This file belongs to the Galois project, a C++ library for exploiting
 * parallelism. The code is being released under the terms of the 3-Clause BSD
 * License (a copy is located in LICENSE.txt at the top-level directory).
 *
 * Copyright (C) 2018, The University of Texas at Austin. All rights reserved.
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

#ifndef KATANA_LIBGALOIS_KATANA_PODVECTOR_H_
#define KATANA_LIBGALOIS_KATANA_PODVECTOR_H_

#include <sys/mman.h>

#include <algorithm>
#include <cassert>
#include <cstddef>
#include <cstring>
#include <iterator>
#include <stdexcept>
#include <type_traits>
#include <utility>

#include "katana/Logging.h"
#include "katana/config.h"

namespace katana {

/**
 * A specialization of std::vector of plain-old-datatype (POD) objects that
 * does not initialize/construct or destruct the objects.
 * (grows allocation in powers of 2 similar to std::vector)
 * Does not support concurrent/scalable or NUMA-aware allocation.
 *
 * Use this when the object type is a POD and when the allocation
 * is done in a serial region but the assignment/construction is done in
 * a parallel region. In other words, when resize() is done on the main thread
 * and values are assigned in parallel (instead of the typical reserve() and
 * emplace_back() on the main thread).
 *
 * If the allocation can be concurrent, check katana::gstl::Vector.
 * If the allocation is large and of known size, then check katana::NUMAArray.
 * Read CONTRIBUTING.md for a more detailed comparison between these types.
 */
template <typename _Tp, bool pinned = false>
class PODVector {
  _Tp* data_;
  size_t capacity_;
  size_t size_;

  constexpr static size_t kMinNonZeroCapacity = 8;

public:
  typedef _Tp value_type;
  typedef size_t size_type;
  typedef ptrdiff_t difference_type;
  typedef value_type& reference;
  typedef const value_type& const_reference;
  typedef value_type* pointer;
  typedef const value_type* const_pointer;
  typedef pointer iterator;
  typedef const_pointer const_iterator;
  typedef std::reverse_iterator<iterator> reverse_iterator;
  typedef std::reverse_iterator<const_iterator> const_reverse_iterator;

  PODVector() : data_(NULL), capacity_(0), size_(0) {}

  template <class InputIterator>
  PODVector(InputIterator first, InputIterator last)
      : data_(NULL), capacity_(0), size_(0) {
    size_t to_add = last - first;
    resize(to_add);
    std::copy_n(first, to_add, begin());
  }

  PODVector(size_t n) : data_(NULL), capacity_(0), size_(0) { resize(n); }

  //! disabled (shallow) copy constructor
  PODVector(const PODVector&) = delete;

  //! move constructor
  PODVector(PODVector&& v)
      : data_(v.data_), capacity_(v.capacity_), size_(v.size_) {
    v.data_ = NULL;
    v.capacity_ = 0;
    v.size_ = 0;
  }

  //! disabled (shallow) copy assignment operator
  PODVector& operator=(const PODVector&) = delete;

  //! move assignment operator
  PODVector& operator=(PODVector&& v) {
    if (data_ != NULL) {
      free(data_);
    }
    data_ = v.data_;
    capacity_ = v.capacity_;
    size_ = v.size_;
    v.data_ = NULL;
    v.capacity_ = 0;
    v.size_ = 0;
    return *this;
  }

  ~PODVector() {
    if (data_ != NULL) {
      free(data_);
    }
  }

  // iterators:
  iterator begin() { return iterator(&data_[0]); }
  const_iterator begin() const { return const_iterator(&data_[0]); }
  iterator end() { return iterator(&data_[size_]); }
  const_iterator end() const { return const_iterator(&data_[size_]); }

  reverse_iterator rbegin() { return reverse_iterator(end()); }
  const_reverse_iterator rbegin() const {
    return const_reverse_iterator(end());
  }
  reverse_iterator rend() { return reverse_iterator(begin()); }
  const_reverse_iterator rend() const {
    return const_reverse_iterator(begin());
  }

  const_iterator cbegin() const { return begin(); }
  const_iterator cend() const { return end(); }
  const_reverse_iterator crbegin() const { return rbegin(); }
  const_reverse_iterator crend() const { return rend(); }

  // size:
  size_type size() const { return size_; }
  size_type max_size() const { return capacity_; }
  bool empty() const { return size_ == 0; }

  void shrink_to_fit() {
    if (size_ == 0) {
      if (data_ != NULL) {
        if constexpr (pinned) {
          munlock(data_, capacity_ * sizeof(_Tp));
        }
        free(data_);
        data_ = NULL;
        capacity_ = 0;
      }
    } else if (size_ < capacity_) {
      if constexpr (pinned) {
        munlock(data_, capacity_ * sizeof(_Tp));
      }
      capacity_ = std::max(size_, kMinNonZeroCapacity);
      const size_t new_bytes = capacity_ * sizeof(_Tp);
      _Tp* new_data_ =
          static_cast<_Tp*>(realloc(reinterpret_cast<void*>(data_), new_bytes));
      KATANA_LOG_DEBUG_ASSERT(new_data_);
      if constexpr (pinned) {
        mlock(new_data_, new_bytes);
      }
      data_ = new_data_;
    }
  }

  void reserve(size_t n) {
    if (n <= capacity_) {
      return;
    }

    // The price of unpinning&pinning again exceeds the savings below
    if constexpr (!pinned) {
      // When reallocing, don't pay for elements greater than size_
      shrink_to_fit();
    }

    [[maybe_unused]] const size_t old_bytes = capacity_ * sizeof(_Tp);

    // reset capacity_ because its previous value need not be a power-of-2
    capacity_ = kMinNonZeroCapacity;
    // increase capacity in powers-of-2
    while (capacity_ < n) {
      capacity_ <<= 1;
    }

    if constexpr (pinned) {
      if (data_ != nullptr) {
        munlock(data_, old_bytes);
      }
    }
    const size_t new_bytes = capacity_ * sizeof(_Tp);
    _Tp* new_data_ =
        static_cast<_Tp*>(realloc(reinterpret_cast<void*>(data_), new_bytes));
    KATANA_LOG_DEBUG_ASSERT(new_data_);
    if constexpr (pinned) {
      mlock(new_data_, new_bytes);
    }
    data_ = new_data_;
  }

  void resize(size_t n) {
    reserve(n);
    size_ = n;
  }

  void clear() { size_ = 0; }

  // element access:
  reference operator[](size_type __n) { return data_[__n]; }
  const_reference operator[](size_type __n) const { return data_[__n]; }
  reference at(size_type __n) {
    if (__n >= size_)
      throw std::out_of_range("PODVector::at");
    return data_[__n];
  }
  const_reference at(size_type __n) const {
    if (__n >= size_)
      throw std::out_of_range("PODVector::at");
    return data_[__n];
  }

  void assign(iterator first, iterator last) {
    size_t n = last - first;
    resize(n);
    memcpy(reinterpret_cast<void*>(data_), first, n * sizeof(_Tp));
  }

  reference front() { return data_[0]; }
  const_reference front() const { return data_[0]; }
  reference back() { return data_[size_ - 1]; }
  const_reference back() const { return data_[size_ - 1]; }

  pointer data() { return data_; }
  const_pointer data() const { return data_; }

  void push_back(const _Tp& value) {
    resize(size_ + 1);
    data_[size_ - 1] = value;
  }

  template <class InputIterator>
  void insert(
      [[maybe_unused]] iterator position, InputIterator first,
      InputIterator last) {
    KATANA_LOG_DEBUG_ASSERT(position == end());
    size_t to_add = last - first;
    if (to_add > 0) {
      size_t old_size = size_;
      resize(old_size + to_add);
      std::copy_n(first, to_add, begin() + old_size);
    }
  }

  void swap(PODVector& v) {
    std::swap(data_, v.data_);
    std::swap(size_, v.size_);
    std::swap(capacity_, v.capacity_);
  }
};

}  // namespace katana
#endif
