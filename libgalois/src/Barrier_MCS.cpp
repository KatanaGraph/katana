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

class MCSBarrier : public katana::Barrier {
  struct TreeNode {
    std::atomic<bool>* parent_pointer;  // null for vpid == 0
    std::atomic<bool>* child_pointers[2];
    bool have_child[4];

    std::atomic<bool> child_not_ready[4];
    std::atomic<bool> parent_sense;
    bool sense;

    TreeNode() = default;

    TreeNode(const TreeNode& rhs)
        : parent_pointer(rhs.parent_pointer), sense(rhs.sense) {
      child_pointers[0] = rhs.child_pointers[0];
      child_pointers[1] = rhs.child_pointers[1];
      for (int i = 0; i < 4; ++i) {
        have_child[i] = rhs.have_child[i];
        child_not_ready[i] = rhs.child_not_ready[i].load();
      }
      parent_sense = rhs.parent_sense.load();
    }
  };

  std::vector<katana::CacheLineStorage<TreeNode>> nodes_;

  void _reinit(unsigned P) {
    nodes_.resize(P);
    for (unsigned i = 0; i < P; ++i) {
      TreeNode& n = nodes_.at(i).get();
      n.sense = true;
      n.parent_sense = false;
      for (int j = 0; j < 4; ++j)
        n.child_not_ready[j] = n.have_child[j] = ((4 * i + j + 1) < P);
      n.parent_pointer =
          (i == 0) ? 0
                   : &nodes_.at((i - 1) / 4).get().child_not_ready[(i - 1) % 4];
      n.child_pointers[0] =
          ((2 * i + 1) >= P) ? 0 : &nodes_.at(2 * i + 1).get().parent_sense;
      n.child_pointers[1] =
          ((2 * i + 2) >= P) ? 0 : &nodes_.at(2 * i + 2).get().parent_sense;
    }
  }

public:
  MCSBarrier(unsigned v) { _reinit(v); }

  void Reinit(unsigned val) override { _reinit(val); }

  void Wait() override {
    TreeNode& n = nodes_.at(katana::ThreadPool::getTID()).get();
    while (n.child_not_ready[0] || n.child_not_ready[1] ||
           n.child_not_ready[2] || n.child_not_ready[3]) {
      katana::asmPause();
    }
    for (int i = 0; i < 4; ++i)
      n.child_not_ready[i] = n.have_child[i];
    if (n.parent_pointer) {
      // FIXME: make sure the compiler doesn't do a RMW because of the as-if
      // rule
      *n.parent_pointer = false;
      while (n.parent_sense != n.sense) {
        katana::asmPause();
      }
    }
    // signal children in wakeup tree
    if (n.child_pointers[0])
      *n.child_pointers[0] = n.sense;
    if (n.child_pointers[1])
      *n.child_pointers[1] = n.sense;
    n.sense = !n.sense;
  }

  const char* name() const override { return "MCSBarrier"; }
};

}  // namespace

std::unique_ptr<katana::Barrier>
katana::CreateMCSBarrier(unsigned active_threads) {
  return std::make_unique<MCSBarrier>(active_threads);
}
