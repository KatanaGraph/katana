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

#include "katana/PerThreadStorage.h"

#include <atomic>
#include <mutex>

#include "katana/PageAlloc.h"
#include "katana/gIO.h"

KATANA_EXPORT thread_local char* katana::ptsBase;

katana::PerBackend&
katana::getPTSBackend() {
  static katana::PerBackend b;
  return b;
}

KATANA_EXPORT thread_local char* katana::pssBase;

katana::PerBackend&
katana::getPPSBackend() {
  static katana::PerBackend b;
  return b;
}

const size_t ptAllocSize = katana::allocSize();
inline void*
alloc() {
  // alloc a single page, don't prefault
  void* toReturn = katana::allocPages(1, true);
  if (toReturn == nullptr) {
    KATANA_DIE("per-thread storage out of memory");
  }
  return toReturn;
}

constexpr unsigned MAX_SIZE = 30;
// PerBackend storage is typically cache-aligned. Simplify bookkeeping at the
// expense of fragmentation by restricting all allocations to be cache-aligned.
constexpr unsigned MIN_SIZE = 7;

static_assert((1 << MIN_SIZE) == katana::KATANA_CACHE_LINE_SIZE);

katana::PerBackend::PerBackend() { freeOffsets.resize(MAX_SIZE); }

unsigned
katana::PerBackend::nextLog2(unsigned size) {
  unsigned i = MIN_SIZE;
  while ((1U << i) < size) {
    ++i;
  }
  if (i >= MAX_SIZE) {
    abort();
  }
  return i;
}

unsigned
katana::PerBackend::allocOffset(const unsigned sz) {
  unsigned ll = nextLog2(sz);
  unsigned size = (1 << ll);

  if (nextLoc.load(std::memory_order_relaxed) + size <= ptAllocSize) {
    // simple path, where we allocate bump ptr style
    unsigned offset = nextLoc.fetch_add(size);
    if (offset + size <= ptAllocSize) {
      return offset;
    }
  }

  if (invalid) {
    KATANA_DIE("allocating after delete");
    return ptAllocSize;
  }

  // find a free offset
  std::lock_guard<Lock> llock(freeOffsetsLock);

  unsigned index = ll;
  if (!freeOffsets[index].empty()) {
    unsigned offset = freeOffsets[index].back();
    freeOffsets[index].pop_back();
    return offset;
  }

  // find a bigger size
  for (; (index < MAX_SIZE) && (freeOffsets[index].empty()); ++index)
    ;

  if (index == MAX_SIZE) {
    KATANA_DIE("per-thread storage out of memory");
    return ptAllocSize;
  }

  // Found a bigger free offset. Use the first piece equal to required
  // size and produce vending machine change for the rest.
  assert(!freeOffsets[index].empty());
  unsigned offset = freeOffsets[index].back();
  freeOffsets[index].pop_back();

  // remaining chunk
  unsigned end = offset + (1 << index);
  unsigned start = offset + size;
  for (unsigned i = index - 1; start < end; --i) {
    freeOffsets[i].push_back(start);
    start += (1 << i);
  }

  assert(offset != ptAllocSize);

  return offset;
}

void
katana::PerBackend::deallocOffset(const unsigned offset, const unsigned sz) {
  unsigned ll = nextLog2(sz);
  unsigned size = (1 << ll);
  unsigned expected = offset + size;

  if (nextLoc.compare_exchange_strong(expected, offset)) {
    // allocation was at the end, so recovered some memory
    return;
  }

  if (invalid) {
    KATANA_DIE("deallocing after delete");
    return;
  }

  // allocation not at the end
  std::lock_guard<Lock> llock(freeOffsetsLock);
  freeOffsets[ll].push_back(offset);
}

void*
katana::PerBackend::getRemote(unsigned thread, unsigned offset) {
  char* rbase = heads[thread].load(std::memory_order_relaxed);
  assert(rbase);
  return &rbase[offset];
}

void
katana::PerBackend::initCommon(unsigned maxT) {
  if (!heads) {
    assert(ThreadPool::getTID() == 0);
    heads = new std::atomic<char*>[maxT] {};
  }
}

char*
katana::PerBackend::initPerThread(unsigned maxT) {
  initCommon(maxT);
  char* b = heads[ThreadPool::getTID()] = (char*)alloc();
  memset(b, 0, ptAllocSize);
  return b;
}

char*
katana::PerBackend::initPerSocket(unsigned maxT) {
  initCommon(maxT);
  unsigned id = ThreadPool::getTID();
  unsigned leader = ThreadPool::getLeader();
  if (id == leader) {
    char* b = heads[id] = (char*)alloc();
    memset(b, 0, ptAllocSize);
    return b;
  }
  char* expected = nullptr;
  // wait for leader to fix up socket
  while (heads[leader].compare_exchange_weak(expected, nullptr)) {
    asmPause();
  }
  heads[id] = heads[leader].load();
  return heads[id];
}

void
katana::initPTS(unsigned maxT) {
  if (!ptsBase) {
    // unguarded initialization as initPTS will run in the master thread
    // before any other threads are generated
    ptsBase = getPTSBackend().initPerThread(maxT);
  }
  if (!pssBase) {
    pssBase = getPPSBackend().initPerSocket(maxT);
  }
}
