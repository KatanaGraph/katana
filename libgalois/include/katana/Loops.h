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

#ifndef KATANA_LIBGALOIS_KATANA_LOOPS_H_
#define KATANA_LIBGALOIS_KATANA_LOOPS_H_

#include "katana/Executor_Deterministic.h"
#include "katana/Executor_DoAll.h"
#include "katana/Executor_ForEach.h"
#include "katana/Executor_OnEach.h"
#include "katana/Executor_Ordered.h"
#include "katana/Executor_ParaMeter.h"
#include "katana/LoopsDecl.h"
#include "katana/WorkList.h"
#include "katana/config.h"

namespace katana {

////////////////////////////////////////////////////////////////////////////////
// Foreach
////////////////////////////////////////////////////////////////////////////////

/**
 * Galois unordered set iterator.
 *
 * Operator should conform to <code>fn(item, UserContext<T>&)</code> where item
 * is a value from the iteration range and T is the type of item.
 *
 * @param range an iterator range typically returned by @ref katana::iterate
 * @param fn operator
 * @param args optional arguments to loop, e.g., {@see loopname}, {@see wl}
 */

template <typename Range, typename FunctionTy, typename... Args>
void
for_each(const Range& range, FunctionTy&& fn, Args&&... args) {
  auto tpl = std::make_tuple(std::forward<Args>(args)...);
  for_each_gen(range, std::forward<FunctionTy>(fn), tpl);
}

/**
 * Standard do-all loop. All iterations should be independent.
 *
 * Operator should conform to <code>fn(item)</code> where item is a value from
 * the iteration range.
 *
 * @param range an iterator range typically returned by @ref katana::iterate
 * @param fn operator
 * @param args optional arguments to loop
 */
template <typename Range, typename FunctionTy, typename... Args>
void
do_all(const Range& range, FunctionTy&& fn, Args&&... args) {
  auto tpl = std::make_tuple(std::forward<Args>(args)...);
  do_all_gen(range, std::forward<FunctionTy>(fn), tpl);
}

/**
 * Low-level parallel loop. Operator is applied for each running thread.
 * Operator should confirm to <code>fn(tid, numThreads)</code> where tid is
 * the id of the current thread and numThreads is the total number of running
 * threads.
 *
 * @param fn operator, which is never copied
 * @param args optional arguments to loop
 */
template <typename FunctionTy, typename... Args>
void
on_each(FunctionTy&& fn, Args&&... args) {
  on_each_gen(
      std::forward<FunctionTy>(fn),
      std::make_tuple(std::forward<Args>(args)...));
}

/**
 * Galois ordered set iterator for stable source algorithms.
 *
 * Operator should conform to <code>fn(item, UserContext<T>&)</code> where item
 * is a value from the iteration range and T is the type of item. Comparison
 * function should conform to <code>bool r = cmp(item1, item2)</code> where r is
 * true if item1 is less than or equal to item2. Neighborhood function should
 * conform to <code>nhFunc(item)</code> and should visit every element in the
 * neighborhood of active element item.
 *
 * @param b begining of range of initial items
 * @param e end of range of initial items
 * @param cmp comparison function
 * @param nhFunc neighborhood function
 * @param fn operator
 * @param loopname string to identity loop in statistics output
 */
template <typename Iter, typename Cmp, typename NhFunc, typename OpFunc>
void
for_each_ordered(
    Iter b, Iter e, const Cmp& cmp, const NhFunc& nhFunc, const OpFunc& fn,
    const char* loopname = 0) {
  for_each_ordered_impl(b, e, cmp, nhFunc, fn, loopname);
}

/**
 * Galois ordered set iterator for unstable source algorithms.
 *
 * Operator should conform to <code>fn(item, UserContext<T>&)</code> where item
 * is a value from the iteration range and T is the type of item. Comparison
 * function should conform to <code>bool r = cmp(item1, item2)</code> where r is
 * true if item1 is less than or equal to item2. Neighborhood function should
 * conform to <code>nhFunc(item)</code> and should visit every element in the
 * neighborhood of active element item. The stability test should conform to
 * <code>bool r = stabilityTest(item)</code> where r is true if item is a stable
 * source.
 *
 * @param b begining of range of initial items
 * @param e end of range of initial items
 * @param cmp comparison function
 * @param nhFunc neighborhood function
 * @param fn operator
 * @param stabilityTest stability test
 * @param loopname string to identity loop in statistics output
 */
template <
    typename Iter, typename Cmp, typename NhFunc, typename OpFunc,
    typename StableTest>
void
for_each_ordered(
    Iter b, Iter e, const Cmp& cmp, const NhFunc& nhFunc, const OpFunc& fn,
    const StableTest& stabilityTest, const char* loopname = 0) {
  for_each_ordered_impl(b, e, cmp, nhFunc, fn, stabilityTest, loopname);
}

/**
 * Helper functor class to invoke katana::do_all on provided args
 * Can be used to choose between katana::do_all and other equivalents such as
 * std::for_each
 */
struct DoAll {
  template <typename Range, typename FunctionTy, typename... Args>
  void operator()(const Range& range, FunctionTy&& fn, Args&&... args) const {
    katana::do_all(
        range, std::forward<FunctionTy>(fn), std::forward<Args>(args)...);
  }
};

/**
 * Helper functor to invoke std::for_each with the same interface as
 * katana::do_all
 */

struct StdForEach {
  template <typename Range, typename FunctionTy, typename... Args>
  void operator()(
      const Range& range, FunctionTy&& fn,
      [[maybe_unused]] Args&&... args) const {
    std::for_each(range.begin(), range.end(), std::forward<FunctionTy>(fn));
  }
};

struct ForEach {
  template <typename Range, typename FunctionTy, typename... Args>
  void operator()(const Range& range, FunctionTy&& fn, Args&&... args) const {
    katana::for_each(
        range, std::forward<FunctionTy>(fn), std::forward<Args>(args)...);
  }
};

template <typename Q>
struct WhileQ {
  Q m_q;

  WhileQ(Q&& q = Q()) : m_q(std::move(q)) {}

  template <typename Range, typename FunctionTy, typename... Args>
  void operator()(
      const Range& range, FunctionTy&& f, [[maybe_unused]] Args&&... args) {
    m_q.push(range.begin(), range.end());

    while (!m_q.empty()) {
      auto val = m_q.pop();

      f(val, m_q);
    }
  }
};

}  // namespace katana

#endif
