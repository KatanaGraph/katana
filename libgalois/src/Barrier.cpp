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

#include "katana/Barrier.h"

#include "katana/Logging.h"
#include "katana/ThreadPool.h"

// anchor vtable
katana::Barrier::~Barrier() = default;

static katana::Barrier* kBarrier = nullptr;
static unsigned kBarrierThreads = 0;

void
katana::internal::SetBarrier(katana::Barrier* barrier) {
  KATANA_LOG_VASSERT(
      !(barrier && kBarrier), "Double initialization of Barrier");

  kBarrier = barrier;

  if (barrier) {
    kBarrierThreads = GetThreadPool().getMaxUsableThreads();
    kBarrier->Reinit(kBarrierThreads);
  }
}

katana::Barrier&
katana::GetBarrier(unsigned active_threads) {
  KATANA_LOG_VASSERT(kBarrier, "Barrier not initialized");
  active_threads =
      std::min(active_threads, GetThreadPool().getMaxUsableThreads());
  active_threads = std::max(active_threads, 1U);

  if (active_threads != kBarrierThreads) {
    kBarrierThreads = active_threads;
    kBarrier->Reinit(kBarrierThreads);
  }

  return *kBarrier;
}
