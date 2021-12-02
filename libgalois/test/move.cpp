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

#include "katana/Bag.h"
#include "katana/FlatMap.h"
#include "katana/Mem.h"
#include "katana/NUMAArray.h"
#include "katana/PerThreadStorage.h"
#include "katana/gdeque.h"
#include "katana/gslist.h"

struct MoveOnly {
  MoveOnly() = default;
  MoveOnly(MoveOnly&&) = default;
  MoveOnly& operator=(MoveOnly&&) = default;
  MoveOnly(const MoveOnly&) = delete;
  MoveOnly& operator=(const MoveOnly&) = delete;
};

struct MoveOnlyA {
  int* x;
  MoveOnlyA() {}
  MoveOnlyA(const MoveOnlyA&) = delete;
  MoveOnly& operator=(const MoveOnlyA&) = delete;
  ~MoveOnlyA() {}
};

template <typename T>
void
test(T&& x) {
  T a = std::move(x);
  T b;
  std::swap(a, b);
  a = std::move(b);
}

template <typename T, typename U>
void
testContainerA(T&& x, U&& y) {
  T a = std::move(x);
  T b;
  b = std::move(a);
  b.emplace_back(std::move(y));
}

template <typename T, typename U>
void
testContainerAA(T&& x, U&& y) {
  katana::FixedSizeHeap heap(sizeof(typename T::block_type));

  T a = std::move(x);
  T b;
  b = std::move(a);
  b.emplace_front(heap, std::move(y));
  b.clear(heap);
}

template <typename T, typename U>
void
testContainerB(T&& x, U&& y) {
  T a = std::move(x);
  T b;
  b = std::move(a);
  b.insert(std::move(y));
}

template <typename T, typename U>
void
testContainerC(T&& x, U&& y) {
  T a = std::move(x);
  T b;
  b = std::move(a);
  b.emplace(b.begin(), std::move(y));
}

int
main() {
  katana::GaloisRuntime Katana_runtime;
  // test(katana::FixedSizeBag<MoveOnly>());
  // test(katana::ConcurrentFixedSizeBag<MoveOnly>());
  // test(katana::FixedSizeRing<MoveOnly>());
  test(katana::gdeque<MoveOnly>());
  test(katana::gslist<MoveOnly>());
  test(katana::concurrent_gslist<MoveOnly>());
  test(katana::InsertBag<MoveOnly>());
  test(katana::NUMAArray<MoveOnly>());
  test(katana::PerSocketStorage<MoveOnly>());
  test(katana::PerThreadStorage<MoveOnly>());

  testContainerA(katana::gdeque<MoveOnly>(), MoveOnly());
  testContainerAA(katana::gslist<MoveOnly>(), MoveOnly());
  // testContainerAA(katana::concurrent_gslist<MoveOnly>(), MoveOnly());
  testContainerA(katana::InsertBag<MoveOnly>(), MoveOnly());
  testContainerC(katana::gdeque<MoveOnly>(), MoveOnly());

  return 0;
}
