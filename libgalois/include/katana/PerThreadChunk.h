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

#ifndef KATANA_LIBGALOIS_KATANA_PERTHREADCHUNK_H_
#define KATANA_LIBGALOIS_KATANA_PERTHREADCHUNK_H_

#include "katana/CompilerSpecific.h"
#include "katana/FixedSizeRing.h"
#include "katana/Mem.h"
#include "katana/PerThreadStorage.h"
#include "katana/PtrLock.h"
#include "katana/Threads.h"
#include "katana/WLCompileCheck.h"

namespace katana {

struct ChunkHeader {
  ChunkHeader* next;
  ChunkHeader* prev;
};

class PerThreadChunkQueue {
  PtrLock<ChunkHeader> head;
  ChunkHeader* tail;

  void prepend(ChunkHeader* C) {
    // Find tail of stolen stuff
    ChunkHeader* t = C;
    while (t->next) {
      t = t->next;
    }
    head.lock();
    t->next = head.getValue();
    if (!t->next)
      tail = t;
    head.unlock_and_set(C);
  }

public:
  PerThreadChunkQueue() : tail(0) {}

  bool empty() const { return !tail; }

  void push(ChunkHeader* obj) {
    head.lock();
    obj->next = 0;
    if (tail) {
      tail->next = obj;
      tail = obj;
      head.unlock();
    } else {
      KATANA_LOG_DEBUG_ASSERT(!head.getValue());
      tail = obj;
      head.unlock_and_set(obj);
    }
  }

  ChunkHeader* pop() {
    // lock free Fast path empty case
    if (empty())
      return 0;

    head.lock();
    ChunkHeader* h = head.getValue();
    if (!h) {
      head.unlock();
      return 0;
    }
    if (tail == h) {
      tail = 0;
      KATANA_LOG_DEBUG_ASSERT(!h->next);
      head.unlock_and_clear();
    } else {
      head.unlock_and_set(h->next);
      h->next = 0;
    }
    return h;
  }

  ChunkHeader* stealAllAndPop(PerThreadChunkQueue& victim) {
    // Don't do work on empty victims (lockfree check)
    if (victim.empty())
      return 0;
    // Steal everything
    victim.head.lock();
    ChunkHeader* C = victim.head.getValue();
    if (C)
      victim.tail = 0;
    victim.head.unlock_and_clear();
    if (!C)
      return 0;  // Didn't get anything
    ChunkHeader* retval = C;
    C = C->next;
    retval->next = 0;
    if (!C)
      return retval;  // Only got one thing
    prepend(C);
    return retval;
  }

  ChunkHeader* stealHalfAndPop(PerThreadChunkQueue& victim) {
    // Don't do work on empty victims (lockfree check)
    if (victim.empty())
      return 0;
    // Steal half
    victim.head.lock();
    ChunkHeader* C = victim.head.getValue();
    ChunkHeader* ntail = C;
    bool count = false;
    while (C) {
      C = C->next;
      if (count)
        ntail = ntail->next;
      count = !count;
    }
    if (ntail) {
      C = ntail->next;
      ntail->next = 0;
      victim.tail = ntail;
    }
    victim.head.unlock();
    if (!C)
      return 0;  // Didn't get anything
    ChunkHeader* retval = C;
    C = C->next;
    retval->next = 0;
    if (!C)
      return retval;  // Only got one thing
    prepend(C);
    return retval;
  }
};

class PerThreadChunkStack {
  PtrLock<ChunkHeader> head;

  void prepend(ChunkHeader* C) {
    // Find tail of stolen stuff
    ChunkHeader* tail = C;
    while (tail->next) {
      tail = tail->next;
    }
    head.lock();
    tail->next = head.getValue();
    head.unlock_and_set(C);
  }

public:
  bool empty() const { return !head.getValue(); }

  void push(ChunkHeader* obj) {
    ChunkHeader* oldhead = 0;
    do {
      oldhead = head.getValue();
      obj->next = oldhead;
    } while (!head.CAS(oldhead, obj));
  }

  ChunkHeader* pop() {
    // lock free Fast empty path
    if (empty())
      return 0;

    // Disable CAS
    head.lock();
    ChunkHeader* retval = head.getValue();
    ChunkHeader* setval = 0;
    if (retval) {
      setval = retval->next;
      retval->next = 0;
    }
    head.unlock_and_set(setval);
    return retval;
  }

  ChunkHeader* stealAllAndPop(PerThreadChunkStack& victim) {
    // Don't do work on empty victims (lockfree check)
    if (victim.empty())
      return 0;
    // Steal everything
    victim.head.lock();
    ChunkHeader* C = victim.head.getValue();
    victim.head.unlock_and_clear();
    if (!C)
      return 0;  // Didn't get anything
    ChunkHeader* retval = C;
    C = C->next;
    retval->next = 0;
    if (!C)
      return retval;  // Only got one thing
    prepend(C);
    return retval;
  }

  ChunkHeader* stealHalfAndPop(PerThreadChunkStack& victim) {
    // Don't do work on empty victims (lockfree check)
    if (victim.empty())
      return 0;
    // Steal half
    victim.head.lock();
    ChunkHeader* C = victim.head.getValue();
    ChunkHeader* ntail = C;
    bool count = false;
    while (C) {
      C = C->next;
      if (count)
        ntail = ntail->next;
      count = !count;
    }
    if (ntail) {
      C = ntail->next;
      ntail->next = 0;
    }
    victim.head.unlock();
    if (!C)
      return 0;  // Didn't get anything
    ChunkHeader* retval = C;
    C = C->next;
    retval->next = 0;
    if (!C)
      return retval;  // Only got one thing
    prepend(C);
    return retval;
  }
};

template <typename InnerWL>
class StealingQueue : private boost::noncopyable {
  PerThreadStorage<std::pair<InnerWL, unsigned>> local;

  KATANA_ATTRIBUTE_NOINLINE
  ChunkHeader* doSteal() {
    std::pair<InnerWL, unsigned>& me = *local.getLocal();
    auto& tp = GetThreadPool();
    unsigned id = tp.getTID();
    unsigned pkg = ThreadPool::getSocket();
    unsigned num = katana::getActiveThreads();

    // First steal from this socket
    for (unsigned eid = id + 1; eid < num; ++eid) {
      if (tp.getSocket(eid) == pkg) {
        ChunkHeader* c = me.first.stealHalfAndPop(local.getRemote(eid)->first);
        if (c)
          return c;
      }
    }
    for (unsigned eid = 0; eid < id; ++eid) {
      if (tp.getSocket(eid) == pkg) {
        ChunkHeader* c = me.first.stealHalfAndPop(local.getRemote(eid)->first);
        if (c)
          return c;
      }
    }

    // Leaders can cross socket
    if (ThreadPool::isLeader()) {
      unsigned eid = (id + me.second) % num;
      ++me.second;
      if (id != eid && tp.isLeader(eid)) {
        ChunkHeader* c = me.first.stealAllAndPop(local.getRemote(eid)->first);
        if (c)
          return c;
      }
    }
    return 0;
  }

public:
  void push(ChunkHeader* c) { local.getLocal()->first.push(c); }

  ChunkHeader* pop() {
    if (ChunkHeader* c = local.getLocal()->first.pop())
      return c;
    return doSteal();
  }
};

template <bool IsLocallyLIFO, int ChunkSize, typename Container, typename T>
struct PerThreadChunkMaster : private boost::noncopyable {
  template <typename _T>
  using retype = PerThreadChunkMaster<IsLocallyLIFO, ChunkSize, Container, _T>;

  template <bool _concurrent>
  using rethread = PerThreadChunkMaster<IsLocallyLIFO, ChunkSize, Container, T>;

  template <int _chunk_size>
  using with_chunk_size =
      PerThreadChunkMaster<IsLocallyLIFO, _chunk_size, Container, T>;

private:
  class Chunk : public ChunkHeader,
                public katana::FixedSizeRing<T, ChunkSize> {};

  FixedSizeAllocator<Chunk> alloc;
  PerThreadStorage<std::pair<Chunk*, Chunk*>> data;
  Container worklist;

  Chunk* mkChunk() {
    Chunk* ptr = alloc.allocate(1);
    alloc.construct(ptr);
    return ptr;
  }

  void delChunk(Chunk* ptr) {
    alloc.destroy(ptr);
    alloc.deallocate(ptr, 1);
  }

  void swapInPush(std::pair<Chunk*, Chunk*>& d) {
    if (!IsLocallyLIFO)
      std::swap(d.first, d.second);
  }

  Chunk*& getPushChunk(std::pair<Chunk*, Chunk*>& d) {
    if (!IsLocallyLIFO)
      return d.second;
    else
      return d.first;
  }

  Chunk*& getPopChunk(std::pair<Chunk*, Chunk*>& d) { return d.first; }

  bool doPush(Chunk* c, const T& val) { return c->push_back(val); }

  std::optional<T> doPop(Chunk* c) {
    if (!IsLocallyLIFO)
      return c->extract_front();
    else
      return c->extract_back();
  }

  void push_internal(std::pair<Chunk*, Chunk*>&, Chunk*& n, const T& val) {
    // Simple case, space in current chunk
    if (n && doPush(n, val))
      return;
    // full chunk, push
    if (n)
      worklist.push(static_cast<ChunkHeader*>(n));
    // get empty chunk;
    n = mkChunk();
    // There better be some room in the new chunk
    doPush(n, val);
  }

public:
  typedef T value_type;

  PerThreadChunkMaster() {}

  void push(value_type val) {
    std::pair<Chunk*, Chunk*>& tld = *data.getLocal();
    Chunk*& n = getPushChunk(tld);
    push_internal(tld, n, val);
  }

  template <typename Iter>
  void push(Iter b, Iter e) {
    std::pair<Chunk*, Chunk*>& tld = *data.getLocal();
    Chunk*& n = getPushChunk(tld);
    while (b != e)
      push_internal(tld, n, *b++);
  }

  template <typename RangeTy>
  void push_initial(const RangeTy& range) {
    push(range.local_begin(), range.local_end());
  }

  std::optional<value_type> pop() {
    std::pair<Chunk*, Chunk*>& tld = *data.getLocal();
    Chunk*& n = getPopChunk(tld);
    std::optional<value_type> retval;
    // simple case, things in current chunk
    if (n && (retval = doPop(n)))
      return retval;
    // empty chunk, trash it
    if (n)
      delChunk(n);
    // get a new chunk
    n = static_cast<Chunk*>(worklist.pop());
    if (n && (retval = doPop(n)))
      return retval;
    // try stealing the push buffer if we can
    swapInPush(tld);
    if (n)
      retval = doPop(n);
    return retval;
  }
};

template <int ChunkSize = 64, typename T = int>
using PerThreadChunkLIFO = PerThreadChunkMaster<
    true, ChunkSize, StealingQueue<PerThreadChunkStack>, T>;
KATANA_WLCOMPILECHECK(PerThreadChunkLIFO)

template <int ChunkSize = 64, typename T = int>
using PerThreadChunkFIFO = PerThreadChunkMaster<
    false, ChunkSize, StealingQueue<PerThreadChunkQueue>, T>;
KATANA_WLCOMPILECHECK(PerThreadChunkFIFO)

}  // namespace katana
#endif
