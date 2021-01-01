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

#ifndef KATANA_LIBGALOIS_KATANA_BULKSYNCHRONOUS_H_
#define KATANA_LIBGALOIS_KATANA_BULKSYNCHRONOUS_H_

#include <atomic>

#include "katana/Barrier.h"
#include "katana/Chunk.h"
#include "katana/WLCompileCheck.h"
#include "katana/config.h"

namespace katana {

/**
 * Bulk-synchronous scheduling. Work is processed in rounds, and all newly
 * created work is processed after all the current work in a round is
 * completed.
 */
template <
    class Container = PerSocketChunkFIFO<>, class T = int,
    bool Concurrent = true>
class BulkSynchronous : private boost::noncopyable {
public:
  template <bool _concurrent>
  using rethread = BulkSynchronous<Container, T, _concurrent>;

  template <typename _T>
  using retype =
      BulkSynchronous<typename Container::template retype<_T>, _T, Concurrent>;

  template <typename _container>
  using with_container = BulkSynchronous<_container, T, Concurrent>;

private:
  typedef typename Container::template rethread<Concurrent> CTy;

  struct TLD {
    unsigned round;
    TLD() : round(0) {}
  };

  CTy wls[2];
  PerThreadStorage<TLD> tlds;
  Barrier& barrier;
  CacheLineStorage<std::atomic<bool>> some;
  std::atomic<bool> isEmpty;

public:
  typedef T value_type;

  BulkSynchronous()
      : barrier(GetBarrier(activeThreads)), some(false), isEmpty(false) {}

  void push(const value_type& val) {
    wls[(tlds.getLocal()->round + 1) & 1].push(val);
  }

  template <typename ItTy>
  void push(ItTy b, ItTy e) {
    while (b != e)
      push(*b++);
  }

  template <typename RangeTy>
  void push_initial(const RangeTy& range) {
    push(range.local_begin(), range.local_end());
    tlds.getLocal()->round = 1;
    some.get() = true;
  }

  katana::optional<value_type> pop() {
    TLD& tld = *tlds.getLocal();
    katana::optional<value_type> r;

    while (true) {
      if (isEmpty)
        return r;  // empty

      r = wls[tld.round].pop();
      if (r)
        return r;

      barrier.Wait();
      if (ThreadPool::getTID() == 0) {
        if (!some.get())
          isEmpty = true;
        some.get() = false;
      }
      tld.round = (tld.round + 1) & 1;
      barrier.Wait();

      r = wls[tld.round].pop();
      if (r) {
        some.get() = true;
        return r;
      }
    }
  }
};
KATANA_WLCOMPILECHECK(BulkSynchronous)

}  // end namespace katana

#endif
