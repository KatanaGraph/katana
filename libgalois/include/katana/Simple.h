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

#ifndef KATANA_LIBGALOIS_KATANA_SIMPLE_H_
#define KATANA_LIBGALOIS_KATANA_SIMPLE_H_

#include <deque>
#include <mutex>

#include "katana/PaddedLock.h"
#include "katana/WLCompileCheck.h"
#include "katana/config.h"
#include "katana/gdeque.h"

namespace katana {

//! Simple Container Wrapper worklist (not scalable).
template <typename T, typename container = std::deque<T>, bool popBack = true>
class Wrapper : private boost::noncopyable {
  PaddedLock<true> lock;
  container wl;

public:
  template <typename _T>
  using retype = Wrapper<_T>;

  template <bool b>
  using rethread = Wrapper;

  typedef T value_type;

  void push(const value_type& val) {
    std::lock_guard<PaddedLock<true>> lg(lock);
    wl.push_back(val);
  }

  template <typename Iter>
  void push(Iter b, Iter e) {
    std::lock_guard<PaddedLock<true>> lg(lock);
    wl.insert(wl.end(), b, e);
  }

  template <typename RangeTy>
  void push_initial(const RangeTy& range) {
    if (ThreadPool::getTID() == 0)
      push(range.begin(), range.end());
  }

  std::optional<value_type> pop() {
    std::optional<value_type> retval;
    std::lock_guard<PaddedLock<true>> lg(lock);
    if (!wl.empty()) {
      if (popBack) {
        retval = wl.back();
        wl.pop_back();
      } else {
        retval = wl.front();
        wl.pop_front();
      }
    }
    return retval;
  }
};

template <typename T = int>
using FIFO = Wrapper<T, std::deque<T>, false>;

template <typename T = int>
using GFIFO = Wrapper<T, katana::gdeque<T>, false>;

template <typename T = int>
using LIFO = Wrapper<T, std::deque<T>, true>;

template <typename T = int>
using GLIFO = Wrapper<T, katana::gdeque<T>, true>;

KATANA_WLCOMPILECHECK(FIFO)
KATANA_WLCOMPILECHECK(GFIFO)
KATANA_WLCOMPILECHECK(LIFO)
KATANA_WLCOMPILECHECK(GLIFO)

}  // end namespace katana

#endif
