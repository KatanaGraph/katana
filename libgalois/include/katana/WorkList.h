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

#ifndef KATANA_LIBGALOIS_KATANA_WORKLIST_H_
#define KATANA_LIBGALOIS_KATANA_WORKLIST_H_

#include "katana/BulkSynchronous.h"
#include "katana/Chunk.h"
#include "katana/LocalQueue.h"
#include "katana/Obim.h"
#include "katana/OrderedList.h"
#include "katana/OwnerComputes.h"
#include "katana/PerThreadChunk.h"
#include "katana/Simple.h"
#include "katana/StableIterator.h"
#include "katana/config.h"
#include "katana/optional.h"

namespace katana {
/**
 * Scheduling policies for Galois iterators. Unless you have very specific
 * scheduling requirement, \ref PerSocketChunkLIFO or \ref PerSocketChunkFIFO is
 * a reasonable scheduling policy. If you need approximate priority scheduling,
 * use \ref OrderedByIntegerMetric. For debugging, you may be interested in
 * \ref FIFO or \ref LIFO, which try to follow serial order exactly.
 *
 * The way to use a worklist is to pass it as a template parameter to
 * \ref for_each(). For example,
 *
 * \code
 * katana::for_each(katana::iterate(beg,end), fn,
 * katana::wl<katana::PerSocketChunkFIFO<32>>());
 * \endcode
 */

namespace {  // don't pollute the symbol table with the example

// Worklists may not be copied.
// All classes (should) conform to:
template <typename T>
class AbstractWorkList {
  AbstractWorkList(const AbstractWorkList&) = delete;
  const AbstractWorkList& operator=(const AbstractWorkList&) = delete;

public:
  AbstractWorkList();

  //! Optional paramaterized Constructor
  //! parameters can be whatever
  AbstractWorkList(int, double, char*);

  //! T is the value type of the WL
  typedef T value_type;

  //! Changes the type the worklist holds
  template <typename _T>
  using retype = AbstractWorkList<_T>;

  //! Pushes a value onto the queue
  void push(const value_type& val);

  //! Pushes a range onto the queue
  template <typename Iter>
  void push(Iter b, Iter e);

  /**
   * Pushes initial range onto the queue. Called with the same b and e on each
   * thread
   */
  template <typename RangeTy>
  void push_initial(const RangeTy&);

  //! Pops a value from the queue.
  katana::optional<value_type> pop();

  /**
   * (optional) Returns true if the worklist is empty. Called infrequently
   * by scheduler after pop has failed. Good way to split retrieving work
   * into pop (fast path) and empty (slow path).
   */
  bool empty();
};

}  // namespace

}  // end namespace katana

#endif
