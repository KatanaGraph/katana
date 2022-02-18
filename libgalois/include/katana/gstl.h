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

#ifndef KATANA_LIBGALOIS_KATANA_GSTL_H_
#define KATANA_LIBGALOIS_KATANA_GSTL_H_

#include <algorithm>
#include <cassert>
#include <deque>
#include <iterator>
#include <list>
#include <map>
#include <set>
#include <sstream>
#include <string>
#include <unordered_set>
#include <utility>
#include <vector>

#include "katana/PriorityQueue.h"
#include "katana/config.h"

namespace katana {

namespace gstl {

//! [STL vector using katana Pow2BlockAllocator]
//! specializes std::vector to use katana concurrent, scaleable allocator:
//! the allocator is composed of thread-local allocators that allocate in
//! multiples of (huge) pages by acquiring a global lock, and divide the pages
//! into a power-of-2 size blocks. Each per-thread allocator maintains a pool
//! of free blocks of different power-of-2 sizes. When an object is allocated,
//! it gets a block from the pool of the thread that allocated it. When an
//! object is deallocated, its block of memory is added to the pool of the
//! thread that deallocated it.
//!
//! Use this when allocations and deallocations can occur in a parallel region.
//! As the memory allocated can be reused for another allocation only by the
//! thread that deallocated it, this is not suitable for use cases where the
//! main thread always does the deallocation (after the parallel region).
//!
//! If the allocation is large and of known size, then check katana::NUMAArray.
//! If the allocation needs to be uninitialized, then check katana::PODVector.
//! Read CONTRIBUTING.md for a more detailed comparison between these types.
template <typename T>
using Vector = std::vector<T, Pow2BlockAllocator<T>>;

//! [STL deque using katana Pow2BlockAllocator]
//! specializes std::deque to use katana concurrent, scaleable allocator:
//! the allocator is composed of thread-local allocators that allocate in
//! multiples of (huge) pages by acquiring a global lock, and divide the pages
//! into a power-of-2 size blocks. Each per-thread allocator maintains a pool
//! of free blocks of different power-of-2 sizes. When an object is allocated,
//! it gets a block from the pool of the thread that allocated it. When an
//! object is deallocated, its block of memory is added to the pool of the
//! thread that deallocated it.
//!
//! Use this when allocations and deallocations can occur in a parallel region.
//! As the memory allocated can be reused for another allocation only by the
//! thread that deallocated it, this is not suitable for use cases where the
//! main thread always does the deallocation (after the parallel region).
template <typename T>
using Deque = std::deque<T, Pow2BlockAllocator<T>>;

//! [STL list using katana FixedSizeAllocator]
//! specializes std::list to use katana concurrent, scaleable allocator:
//! the allocator is composed of thread-local allocators that allocate in
//! multiples of (huge) pages by acquiring a global lock, and divide the pages
//! into fixed size blocks. Each per-thread allocator maintains a pool of free
//! blocks. When an object is allocated, it gets a block from the pool of the
//! thread that allocated it. When an object is deallocated, its block of
//! memory is added to the pool of the thread that deallocated it.
//!
//! Use this when allocations and deallocations can occur in a parallel region.
//! As the memory allocated can be reused for another allocation only by the
//! thread that deallocated it, this is not suitable for use cases where the
//! main thread always does the deallocation (after the parallel region).
template <typename T>
using List = std::list<T, FixedSizeAllocator<T>>;

//! [STL set using katana FixedSizeAllocator]
//! specializes std::set to use katana concurrent, scaleable allocator:
//! the allocator is composed of thread-local allocators that allocate in
//! multiples of (huge) pages by acquiring a global lock, and divide the pages
//! into fixed size blocks. Each per-thread allocator maintains a pool of free
//! blocks. When an object is allocated, it gets a block from the pool of the
//! thread that allocated it. When an object is deallocated, its block of
//! memory is added to the pool of the thread that deallocated it.
//!
//! Use this when allocations and deallocations can occur in a parallel region.
//! As the memory allocated can be reused for another allocation only by the
//! thread that deallocated it, this is not suitable for use cases where the
//! main thread always does the deallocation (after the parallel region).
template <typename T, typename C = std::less<T>>
using Set = std::set<T, C, FixedSizeAllocator<T>>;

//! [STL multiset using katana FixedSizeAllocator]
//! specializes std::multiset to use katana concurrent, scaleable allocator:
//! the allocator is composed of thread-local allocators that allocate in
//! multiples of (huge) pages by acquiring a global lock, and divide the pages
//! into fixed size blocks. Each per-thread allocator maintains a pool of free
//! blocks. When an object is allocated, it gets a block from the pool of the
//! thread that allocated it. When an object is deallocated, its block of
//! memory is added to the pool of the thread that deallocated it.
//!
//! Use this when allocations and deallocations can occur in a parallel region.
//! As the memory allocated can be reused for another allocation only by the
//! thread that deallocated it, this is not suitable for use cases where the
//! main thread always does the deallocation (after the parallel region).
template <typename T, typename C = std::less<T>>
using MultiSet = std::multiset<T, C, FixedSizeAllocator<T>>;

//! [STL unordered_set using katana Pow2BlockAllocator]
//! specializes std::unordered_set to use katana concurrent, scaleable allocator:
//! the allocator is composed of thread-local allocators that allocate in
//! multiples of (huge) pages by acquiring a global lock, and divide the pages
//! into a power-of-2 size blocks. Each per-thread allocator maintains a pool
//! of free blocks of different power-of-2 sizes. When an object is allocated,
//! it gets a block from the pool of the thread that allocated it. When an
//! object is deallocated, its block of memory is added to the pool of the
//! thread that deallocated it.
//!
//! Use this when allocations and deallocations can occur in a parallel region.
//! As the memory allocated can be reused for another allocation only by the
//! thread that deallocated it, this is not suitable for use cases where the
//! main thread always does the deallocation (after the parallel region).
template <
    typename T, typename Hash = std::hash<T>,
    typename KeyEqual = std::equal_to<T>>
using UnorderedSet =
    std::unordered_set<T, Hash, KeyEqual, Pow2BlockAllocator<T>>;

//! [STL map using katana FixedSizeAllocator]
//! specializes std::map to use katana concurrent, scaleable allocator:
//! the allocator is composed of thread-local allocators that allocate in
//! multiples of (huge) pages by acquiring a global lock, and divide the pages
//! into fixed size blocks. Each per-thread allocator maintains a pool of free
//! blocks. When an object is allocated, it gets a block from the pool of the
//! thread that allocated it. When an object is deallocated, its block of
//! memory is added to the pool of the thread that deallocated it.
//!
//! Use this when allocations and deallocations can occur in a parallel region.
//! As the memory allocated can be reused for another allocation only by the
//! thread that deallocated it, this is not suitable for use cases where the
//! main thread always does the deallocation (after the parallel region).
template <typename K, typename V, typename C = std::less<K>>
using Map = std::map<K, V, C, FixedSizeAllocator<std::pair<const K, V>>>;

//! [STL unordered_map using katana Pow2BlockAllocator]
//! specializes std::unordered_map to use katana concurrent, scaleable allocator:
//! the allocator is composed of thread-local allocators that allocate in
//! multiples of (huge) pages by acquiring a global lock, and divide the pages
//! into a power-of-2 size blocks. Each per-thread allocator maintains a pool
//! of free blocks of different power-of-2 sizes. When an object is allocated,
//! it gets a block from the pool of the thread that allocated it. When an
//! object is deallocated, its block of memory is added to the pool of the
//! thread that deallocated it.
//!
//! Use this when allocations and deallocations can occur in a parallel region.
//! As the memory allocated can be reused for another allocation only by the
//! thread that deallocated it, this is not suitable for use cases where the
//! main thread always does the deallocation (after the parallel region).
template <
    typename K, typename V, typename Hash = std::hash<K>,
    typename KeyEqual = std::equal_to<K>>
using UnorderedMap = std::unordered_map<
    K, V, Hash, KeyEqual, Pow2BlockAllocator<std::pair<const K, V>>>;

//! [STL basic_string using katana Pow2BlockAllocator]
//! specializes std::basic_string to use katana concurrent, scaleable allocator:
//! the allocator is composed of thread-local allocators
//! that manage thread-local pages and only use a global lock to
//! get allocation in chunks of (huge) pages.
//! When destroyed, its memory is reclaimed by the thread that destroys it.
//!
//! Use this when allocations and deallocations can occur in a parallel region.
//! As the memory allocated can be reused for another allocation only by the
//! thread that deallocated it, this is not suitable for use cases where the
//! main thread always does the deallocation (after the parallel region).
using Str =
    std::basic_string<char, std::char_traits<char>, Pow2BlockAllocator<char>>;

template <typename T>
struct StrMaker {
  Str operator()(const T& x) const {
    std::basic_ostringstream<
        char, std::char_traits<char>, Pow2BlockAllocator<char>>
        os;
    os << x;
    return Str(os.str());
  }
};

template <>
struct StrMaker<std::string> {
  Str operator()(const std::string& x) const { return Str(x.begin(), x.end()); }
};

template <>
struct StrMaker<Str> {
  const Str& operator()(const Str& x) const { return x; }
};

template <>
struct StrMaker<const char*> {
  Str operator()(const char* x) const { return Str(x); }
};

template <typename T>
Str
makeStr(const T& x) {
  return StrMaker<T>()(x);
}
}  // end namespace gstl

template <typename IterTy, class Distance>
IterTy
safe_advance_dispatch(
    IterTy b, IterTy e, Distance n, std::random_access_iterator_tag) {
  if (std::distance(b, e) >= n)
    return b + n;
  else
    return e;
}

template <typename IterTy, class Distance>
IterTy
safe_advance_dispatch(IterTy b, IterTy e, Distance n, std::input_iterator_tag) {
  while (b != e && n--)
    ++b;
  return b;
}

/**
 * Like std::advance but returns end if end is closer than the advance amount.
 */
template <typename IterTy, class Distance>
IterTy
safe_advance(IterTy b, IterTy e, Distance n) {
  typename std::iterator_traits<IterTy>::iterator_category category;
  return safe_advance_dispatch(b, e, n, category);
}

namespace internal {
template <typename I>
using IteratorValueType = typename std::iterator_traits<I>::value_type;
}  // namespace internal

//! Destroy a range
template <typename I>
std::enable_if_t<!std::is_scalar<internal::IteratorValueType<I>>::value>
uninitialized_destroy(I first, I last) {
  using T = internal::IteratorValueType<I>;
  for (; first != last; ++first)
    (&*first)->~T();
}

template <class I>
std::enable_if_t<std::is_scalar<internal::IteratorValueType<I>>::value>
uninitialized_destroy(I, I) {}

}  // namespace katana
#endif
