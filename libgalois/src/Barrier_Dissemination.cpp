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

#include <atomic>

#include "katana/Barrier.h"
#include "katana/CompilerSpecific.h"
#include "katana/ThreadPool.h"

namespace {

#define FAST_LOG2(x)                                                           \
  (sizeof(unsigned long) * 8 - 1 - __builtin_clzl((unsigned long)(x)))
#define FAST_LOG2_UP(x)                                                        \
  (((x) - (1 << FAST_LOG2(x))) ? FAST_LOG2(x) + 1 : FAST_LOG2(x))

class DisseminationBarrier : public katana::Barrier {
  struct node {
    std::atomic<int> flag[2];
    node* partner;
    node() : partner(nullptr) {}
    node(const node& rhs) : partner(rhs.partner) {
      flag[0] = rhs.flag[0].load();
      flag[1] = rhs.flag[1].load();
    }
  };

  struct LocalData {
    int parity;
    int sense;
    node myflags[32];
  };

  std::vector<katana::CacheLineStorage<LocalData>> nodes_;
  unsigned log_p_;

  void _reinit(unsigned P) {
    log_p_ = FAST_LOG2_UP(P);
    nodes_.resize(P);
    for (unsigned i = 0; i < P; ++i) {
      LocalData& lhs = nodes_.at(i).get();
      lhs.parity = 0;
      lhs.sense = 1;
      for (unsigned j = 0; j < sizeof(lhs.myflags) / sizeof(*lhs.myflags);
           ++j) {
        lhs.myflags[j].flag[0] = lhs.myflags[j].flag[1] = 0;
      }

      int d = 1;
      for (unsigned j = 0; j < log_p_; ++j) {
        LocalData& rhs = nodes_.at((i + d) % P).get();
        lhs.myflags[j].partner = &rhs.myflags[j];
        d *= 2;
      }
    }
  }

public:
  DisseminationBarrier(unsigned v) { _reinit(v); }

  void Reinit(unsigned val) override { _reinit(val); }

  void Wait() override {
    auto& ld = nodes_.at(katana::ThreadPool::getTID()).get();
    auto& sense = ld.sense;
    auto& parity = ld.parity;
    for (unsigned r = 0; r < log_p_; ++r) {
      ld.myflags[r].partner->flag[parity] = sense;
      while (ld.myflags[r].flag[parity] != sense) {
        katana::asmPause();
      }
    }
    if (parity == 1) {
      sense = 1 - ld.sense;
    }
    parity = 1 - parity;
  }

  const char* name() const override { return "DisseminationBarrier"; }
};

}  // namespace

std::unique_ptr<katana::Barrier>
katana::CreateDisseminationBarrier(unsigned active_threads) {
  return std::make_unique<DisseminationBarrier>(active_threads);
}
