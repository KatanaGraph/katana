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

#include "galois/runtime/Mem.h"

#include <map>
#include <mutex>

#include "galois/Mem.h"
#include "galois/runtime/Executor_OnEach.h"

void
galois::Prealloc(size_t pagesPerThread, size_t bytes) {
  size_t allocSize = (pagesPerThread * galois::runtime::activeThreads) +
                     (bytes / substrate::allocSize());
  // If the user requested a non-zero allocation, at the very least
  // allocate a page.
  if (allocSize == 0 && bytes > 0) {
    allocSize = 1;
  }

  galois::Prealloc(allocSize);
}

void
galois::Prealloc(size_t pages) {
  unsigned pagesPerThread = (pages + galois::runtime::activeThreads - 1) /
                            galois::runtime::activeThreads;
  galois::substrate::getThreadPool().run(galois::runtime::activeThreads, [=]() {
    galois::substrate::pagePoolPreAlloc(pagesPerThread);
  });
}

// Anchor the class
galois::runtime::SystemHeap::SystemHeap() {
  assert(AllocSize == galois::substrate::allocSize());
}

galois::runtime::SystemHeap::~SystemHeap() = default;

thread_local galois::runtime::SizedHeapFactory::HeapMap*
    galois::runtime::SizedHeapFactory::localHeaps = nullptr;

galois::runtime::SizedHeapFactory::SizedHeap*
galois::runtime::SizedHeapFactory::getHeapForSize(const size_t size) {
  if (size == 0) {
    return nullptr;
  }
  return Base::getInstance()->getHeap(size);
}

galois::runtime::SizedHeapFactory::SizedHeap*
galois::runtime::SizedHeapFactory::getHeap(const size_t size) {
  typedef SizedHeapFactory::HeapMap HeapMap;

  if (!localHeaps) {
    std::lock_guard<galois::substrate::SimpleLock> ll(lock);
    localHeaps = new HeapMap;
    allLocalHeaps.push_front(localHeaps);
  }

  auto& lentry = (*localHeaps)[size];
  if (lentry) {
    return lentry;
  }

  {
    std::lock_guard<galois::substrate::SimpleLock> ll(lock);
    auto& gentry = heaps[size];
    if (!gentry) {
      gentry = new SizedHeap();
    }
    lentry = gentry;
    return lentry;
  }
}

galois::runtime::Pow2BlockHeap::Pow2BlockHeap() noexcept { populateTable(); }

galois::runtime::SizedHeapFactory::SizedHeapFactory() = default;

galois::runtime::SizedHeapFactory::~SizedHeapFactory() {
  // TODO destructor ordering problem: there may be pointers to deleted
  // SizedHeap when this Factory is destroyed before dependent
  // FixedSizeHeaps.
  for (const auto& entry : heaps) {
    delete entry.second;
  }
  for (const auto& mptr : allLocalHeaps) {
    delete mptr;
  }
}
