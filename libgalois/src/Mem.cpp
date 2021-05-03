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

#include "katana/Mem.h"

#include <map>
#include <mutex>

#include "katana/Executor_OnEach.h"
#include "katana/Mem.h"

void
katana::Prealloc(size_t pagesPerThread, size_t bytes) {
  size_t size =
      (pagesPerThread * katana::activeThreads) + (bytes / allocSize());
  // If the user requested a non-zero allocation, at the very least
  // allocate a page.
  if (size == 0 && bytes > 0) {
    size = 1;
  }

  katana::Prealloc(size);
}

void
katana::Prealloc(size_t pages) {
  unsigned pagesPerThread =
      (pages + katana::activeThreads - 1) / katana::activeThreads;
  katana::GetThreadPool().run(katana::activeThreads, [=]() {
    katana::pagePoolPreAlloc(pagesPerThread);
  });
}

void
katana::EnsurePreallocated(size_t pagesPerThread, size_t bytes) {
  size_t size =
      (pagesPerThread * katana::activeThreads) + (bytes / allocSize());
  // If the user requested a non-zero allocation, at the very least
  // allocate a page.
  if (size == 0 && bytes > 0) {
    size = 1;
  }

  katana::EnsurePreallocated(size);
}

void
katana::EnsurePreallocated(size_t pages) {
  unsigned pagesPerThread =
      (pages + katana::activeThreads - 1) / katana::activeThreads;
  katana::GetThreadPool().run(katana::activeThreads, [=]() {
    katana::pagePoolEnsurePreallocated(pagesPerThread);
  });
}

// Anchor the class
katana::SystemHeap::SystemHeap() {
  KATANA_LOG_DEBUG_ASSERT(AllocSize == katana::allocSize());
}

katana::SystemHeap::~SystemHeap() = default;

thread_local katana::SizedHeapFactory::HeapMap*
    katana::SizedHeapFactory::localHeaps = nullptr;

katana::SizedHeapFactory::SizedHeap*
katana::SizedHeapFactory::getHeapForSize(const size_t size) {
  if (size == 0) {
    return nullptr;
  }
  return Base::getInstance()->getHeap(size);
}

katana::SizedHeapFactory::SizedHeap*
katana::SizedHeapFactory::getHeap(const size_t size) {
  typedef SizedHeapFactory::HeapMap HeapMap;

  if (!localHeaps) {
    std::lock_guard<katana::SimpleLock> ll(lock);
    localHeaps = new HeapMap;
    allLocalHeaps.push_front(localHeaps);
  }

  auto& lentry = (*localHeaps)[size];
  if (lentry) {
    return lentry;
  }

  {
    std::lock_guard<katana::SimpleLock> ll(lock);
    auto& gentry = heaps[size];
    if (!gentry) {
      gentry = new SizedHeap();
    }
    lentry = gentry;
    return lentry;
  }
}

katana::Pow2BlockHeap::Pow2BlockHeap() noexcept { populateTable(); }

katana::SizedHeapFactory::SizedHeapFactory() = default;

katana::SizedHeapFactory::~SizedHeapFactory() {
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
