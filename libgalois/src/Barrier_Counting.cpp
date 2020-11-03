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

#include "galois/substrate/Barrier.h"
#include "galois/substrate/CompilerSpecific.h"
#include "galois/substrate/ThreadPool.h"

namespace {

class CountingBarrier : public galois::substrate::Barrier {
  std::atomic<unsigned> count_;
  std::atomic<bool> sense_;
  unsigned num_;
  std::vector<galois::substrate::CacheLineStorage<bool>> local_sense_;

  void _reinit(unsigned val) {
    count_ = num_ = val;
    sense_ = false;
    local_sense_.resize(val);
    for (unsigned i = 0; i < val; ++i) {
      local_sense_.at(i).get() = false;
    }
  }

public:
  CountingBarrier(unsigned int active_threads) { _reinit(active_threads); }

  void Reinit(unsigned val) override { _reinit(val); }

  void Wait() override {
    bool& lsense =
        local_sense_.at(galois::substrate::ThreadPool::getTID()).get();
    lsense = !lsense;
    if (--count_ == 0) {
      count_ = num_;
      sense_ = lsense;
    } else {
      while (sense_ != lsense) {
        galois::substrate::asmPause();
      }
    }
  }

  const char* name() const override { return "CountingBarrier"; }
};

}  // namespace

std::unique_ptr<galois::substrate::Barrier>
galois::substrate::CreateCountingBarrier(unsigned active_threads) {
  return std::make_unique<CountingBarrier>(active_threads);
}
