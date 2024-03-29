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

#include <cassert>
#include <deque>
#include <iostream>
#include <random>
#include <string>
#include <vector>

#include <boost/iterator/counting_iterator.hpp>

#include "katana/Bag.h"
#include "katana/Galois.h"
#include "katana/Mem.h"
#include "katana/Timer.h"
#include "katana/gIO.h"
#include "katana/gdeque.h"
#include "katana/gslist.h"

template <typename C>
auto constexpr needs_heap(int)
    -> decltype(typename C::promise_to_dealloc(), bool()) {
  return true;
}

template <typename C>
bool constexpr needs_heap(...) {
  return false;
}

template <typename C, typename HeapTy, typename V>
auto
addToCollection(C& c, HeapTy& heap, V&& v) ->
    typename std::enable_if<needs_heap<C>(0)>::type {
  c.push_front(heap.heap, std::forward<V>(v));
}

template <typename C, typename HeapTy, typename V>
auto
addToCollection(C& c, HeapTy&, V&& v) ->
    typename std::enable_if<!needs_heap<C>(0)>::type {
  c.push_back(std::forward<V>(v));
}

template <typename C>
auto
removeFromCollection(C& c) -> typename std::enable_if<needs_heap<C>(0)>::type {
  c.pop_front(typename C::promise_to_dealloc());
}

template <typename C>
auto
removeFromCollection(C& c) -> typename std::enable_if<!needs_heap<C>(0)>::type {
  c.pop_back();
}

template <typename C, bool Enable>
struct Heap {};

template <typename C>
struct Heap<C, true> {
  katana::FixedSizeHeap heap;
  Heap() : heap(sizeof(typename C::block_type)) {}
};

template <typename C>
void
testBasic(std::string prefix, C&& collection, int N) {
  Heap<C, needs_heap<C>(0)> heap;

  assert(N > 0);
  C c = std::move(collection);
  for (int i = 0; i < N; ++i)
    addToCollection(c, heap, i);

  int i = 0;
  for (auto it = c.begin(); it != c.end(); ++it, ++i) {
    ;
  }

  KATANA_LOG_VASSERT(N == std::distance(c.begin(), c.end()), "{}", prefix);

  i = N - 1;
  for (; !c.empty(); --i, removeFromCollection(c)) {
    ;
  }

  KATANA_LOG_VASSERT(0 == std::distance(c.begin(), c.end()), "{}", prefix);
}

template <typename C>
void
testNormal(std::string prefix, C&& collection, int N) {
  Heap<C, needs_heap<C>(0)> heap;

  assert(N > 0);
  C c = std::move(collection);
  for (int i = 0; i < N; ++i)
    addToCollection(c, heap, i);

  int i = 0;
  for (auto it = c.begin(); it != c.end(); ++it, ++i) {
    KATANA_LOG_VASSERT(*it == i, "{}", prefix);
  }

  i = N - 1;
  for (auto it = c.rbegin(); it != c.rend(); ++it, --i) {
    KATANA_LOG_VASSERT(*it == i, "{}", prefix);
  }

  KATANA_LOG_VASSERT(static_cast<int>(c.size()) == N, "{}", prefix);

  KATANA_LOG_VASSERT(
      static_cast<int>(c.size()) == std::distance(c.begin(), c.end()), "{}",
      prefix);

  i = N - 1;
  for (; !c.empty(); --i, removeFromCollection(c)) {
    KATANA_LOG_VASSERT(c.back() == i, "{}", prefix);
  }

  KATANA_LOG_VASSERT(static_cast<int>(c.size()) == 0, "{}", prefix);
  KATANA_LOG_VASSERT(
      static_cast<int>(c.size()) == std::distance(c.begin(), c.end()), "{}",
      prefix);
}

template <typename C>
void
testSort(std::string prefix, C&& collection, int N) {
  Heap<C, needs_heap<C>(0)> heap;

  assert(N > 0);
  C c = std::move(collection);
  std::mt19937 gen;
  std::uniform_int_distribution<int> dist(0, 100);
  for (int i = 0; i < N; ++i)
    addToCollection(c, heap, dist(gen));

  std::sort(c.begin(), c.end());

  int last = c.front();
  for (auto it = c.begin() + 1; it != c.end(); ++it) {
    KATANA_LOG_VASSERT(last <= *it, "{}", prefix);
    last = *it;
  }

  last = c.back();
  removeFromCollection(c);
  for (; !c.empty(); removeFromCollection(c)) {
    KATANA_LOG_VASSERT(last >= c.back(), "{}", prefix);
    last = c.back();
  }
}

template <typename C, typename Iterator>
void
timeAccess(std::string prefix, C&& c, Iterator first, Iterator last) {
  Heap<C, needs_heap<C>(0)> heap;

  katana::Timer t1, t2;
  t1.start();
  while (first != last) {
    addToCollection(c, heap, *first++);
  }
  t1.stop();
  t2.start();
  for (auto ii = c.begin(), ei = c.end(); ii != ei; ++ii) {
    (*ii).val;
  }
  t2.stop();
  std::cout << prefix << " insert: " << t1.get() << " traverse: " << t2.get()
            << "\n";
}

template <typename T>
void
timeAccesses(std::string prefix, T&& x, int size) {
  for (int i = 0; i < 3; ++i)
    timeAccess(
        prefix, std::forward<T>(x), boost::counting_iterator<int>(0),
        boost::counting_iterator<int>(size));
}

struct element {
  volatile int val;
  element(int x) : val(x) {}
};

int
main(int argc, char** argv) {
  katana::GaloisRuntime Katana_runtime;
  testBasic("katana::gslist", katana::gslist<int>(), 32 * 32);
  testNormal("katana::gdeque", katana::gdeque<int>(), 32 * 32);
  // testSort("katana::gdeque", katana::gdeque<int>(), 32 * 32);

  int size = 100;
  if (argc > 1)
    size = atoi(argv[1]);
  if (size <= 0)
    size = 1000000;
  timeAccesses("std::deque", std::deque<element>(), size);
  timeAccesses("std::vector", std::vector<element>(), size);
  timeAccesses("katana::gdeque", katana::gdeque<element>(), size);
  timeAccesses("katana::gslist", katana::gslist<element>(), size);
  timeAccesses(
      "katana::concurrent_gslist", katana::concurrent_gslist<element>(), size);
  timeAccesses("katana::InsertBag", katana::InsertBag<element>(), size);

  return 0;
}
