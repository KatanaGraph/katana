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

#ifndef KATANA_LIBGALOIS_KATANA_PAGEPOOL_H_
#define KATANA_LIBGALOIS_KATANA_PAGEPOOL_H_

#include <cstddef>
#include <deque>
#include <mutex>
#include <numeric>
#include <unordered_map>
#include <vector>

#include "katana/CacheLineStorage.h"
#include "katana/PageAlloc.h"
#include "katana/PtrLock.h"
#include "katana/SimpleLock.h"
#include "katana/ThreadPool.h"
#include "katana/config.h"

namespace katana {

//! Low level page pool (individual pages, use largeMalloc for large blocks)

KATANA_EXPORT void* pagePoolAlloc();
KATANA_EXPORT void pagePoolFree(void*);
KATANA_EXPORT void pagePoolPreAlloc(unsigned);

//! Returns total large pages allocated by Galois memory management subsystem
KATANA_EXPORT int numPagePoolAllocTotal();
//! Returns total large pages allocated for thread by Galois memory management
//! subsystem
KATANA_EXPORT int numPagePoolAllocForThread(unsigned tid);

namespace internal {

struct FreeNode {
  FreeNode* next;
};

typedef katana::PtrLock<FreeNode> HeadPtr;
typedef katana::CacheLineStorage<HeadPtr> HeadPtrStorage;

// Tracks pages allocated
template <typename _UNUSED = void>
class PageAllocState {
  std::deque<std::atomic<int>> counts;
  std::vector<HeadPtrStorage> pool;
  std::unordered_map<void*, int> ownerMap;
  katana::SimpleLock mapLock;

  void* allocFromOS() {
    void* ptr = katana::allocPages(1, true);
    KATANA_LOG_DEBUG_ASSERT(ptr);
    auto tid = katana::ThreadPool::getTID();
    counts[tid] += 1;
    std::lock_guard<katana::SimpleLock> lg(mapLock);
    ownerMap[ptr] = tid;
    return ptr;
  }

public:
  PageAllocState() {
    auto num = katana::GetThreadPool().getMaxThreads();
    counts.resize(num);
    pool.resize(num);
  }

  int count(unsigned tid) const { return counts[tid]; }

  int countAll() const {
    return std::accumulate(counts.begin(), counts.end(), 0);
  }

  void* pageAlloc() {
    auto tid = katana::ThreadPool::getTID();
    HeadPtr& hp = pool[tid].data;
    if (hp.getValue()) {
      hp.lock();
      FreeNode* h = hp.getValue();
      if (h) {
        hp.unlock_and_set(h->next);
        return h;
      }
      hp.unlock();
    }
    return allocFromOS();
  }

  void pageFree(void* ptr) {
    KATANA_LOG_DEBUG_ASSERT(ptr);
    mapLock.lock();
    KATANA_LOG_DEBUG_ASSERT(ownerMap.count(ptr));
    int i = ownerMap[ptr];
    mapLock.unlock();
    HeadPtr& hp = pool[i].data;
    hp.lock();
    FreeNode* nh = reinterpret_cast<FreeNode*>(ptr);
    nh->next = hp.getValue();
    hp.unlock_and_set(nh);
  }

  void pagePreAlloc() { pageFree(allocFromOS()); }
};

//! Initialize PagePool, used by init();
KATANA_EXPORT void setPagePoolState(PageAllocState<>* pa);

}  // end namespace internal

}  // end namespace katana

#endif
