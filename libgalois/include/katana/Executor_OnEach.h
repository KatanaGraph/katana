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

#ifndef KATANA_LIBGALOIS_KATANA_EXECUTORONEACH_H_
#define KATANA_LIBGALOIS_KATANA_EXECUTORONEACH_H_

#include "katana/OperatorReferenceTypes.h"
#include "katana/ThreadPool.h"
#include "katana/ThreadTimer.h"
#include "katana/Threads.h"
#include "katana/Timer.h"
#include "katana/Traits.h"
#include "katana/config.h"
#include "katana/gIO.h"

namespace katana {

namespace internal {

template <typename FunctionTy, typename ArgsTy>
inline void
on_each_impl(FunctionTy&& fn, const ArgsTy& argsTuple) {
  static_assert(!has_trait<char*, ArgsTy>(), "old loopname");
  static_assert(!has_trait<char const*, ArgsTy>(), "old loopname");

  static constexpr bool NEEDS_STATS = has_trait<loopname_tag, ArgsTy>();
  static constexpr bool MORE_STATS =
      NEEDS_STATS && has_trait<more_stats_tag, ArgsTy>();

  const char* const loopname = katana::internal::getLoopName(argsTuple);

  CondStatTimer<NEEDS_STATS> timer(loopname);

  PerThreadTimer<MORE_STATS> execTime(loopname, "Execute");

  const auto numT = getActiveThreads();

  OperatorReferenceType<decltype(std::forward<FunctionTy>(fn))> fn_ref = fn;

  auto runFun = [&] {
    execTime.start();

    fn_ref(ThreadPool::getTID(), numT);

    execTime.stop();
  };

  timer.start();
  GetThreadPool().run(numT, runFun);
  timer.stop();
}

}  // namespace internal

template <typename FunctionTy, typename TupleTy>
inline void
on_each_gen(FunctionTy&& fn, const TupleTy& tpl) {
  internal::on_each_impl(std::forward<FunctionTy>(fn), tpl);
}

}  // end namespace katana

#endif
