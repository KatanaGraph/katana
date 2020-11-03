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

#include "galois/substrate/Barrier.h"
#include "galois/substrate/CompilerSpecific.h"
#include "galois/substrate/PerThreadStorage.h"

namespace {

class TopoBarrier : public galois::substrate::Barrier {
  struct TreeNode {
    // socket binary tree
    TreeNode* parent_pointer;  // null of vpid == 0
    TreeNode* child_pointers[2];

    // waiting values:
    unsigned have_child;
    std::atomic<unsigned> child_not_ready;

    // signal values
    std::atomic<unsigned> parent_sense;
  };

  galois::substrate::PerSocketStorage<TreeNode> nodes_;
  galois::substrate::PerThreadStorage<unsigned> sense_;

  void _reinit(unsigned P) {
    auto& tp = galois::substrate::GetThreadPool();
    unsigned pkgs = tp.getCumulativeMaxSocket(P - 1) + 1;
    for (unsigned i = 0; i < pkgs; ++i) {
      TreeNode& n = *nodes_.getRemoteByPkg(i);
      n.child_not_ready = 0;
      n.have_child = 0;
      for (int j = 0; j < 4; ++j) {
        if ((4 * i + j + 1) < pkgs) {
          ++n.child_not_ready;
          ++n.have_child;
        }
      }
      for (unsigned j = 0; j < P; ++j) {
        if (tp.getSocket(j) == i && !tp.isLeader(j)) {
          ++n.child_not_ready;
          ++n.have_child;
        }
      }
      n.parent_pointer = (i == 0) ? 0 : nodes_.getRemoteByPkg((i - 1) / 4);
      n.child_pointers[0] =
          ((2 * i + 1) >= pkgs) ? 0 : nodes_.getRemoteByPkg(2 * i + 1);
      n.child_pointers[1] =
          ((2 * i + 2) >= pkgs) ? 0 : nodes_.getRemoteByPkg(2 * i + 2);
      n.parent_sense = 0;
    }
    for (unsigned i = 0; i < P; ++i) {
      *sense_.getRemote(i) = 1;
    }
  }

public:
  TopoBarrier(unsigned v) { _reinit(v); }

  // not safe if any thread is in wait
  void Reinit(unsigned val) override { _reinit(val); }

  void Wait() override {
    unsigned id = galois::substrate::ThreadPool::getTID();
    TreeNode& n = *nodes_.getLocal();
    unsigned& s = *sense_.getLocal();
    bool leader = galois::substrate::ThreadPool::isLeader();
    // completion tree
    if (leader) {
      while (n.child_not_ready) {
        galois::substrate::asmPause();
      }
      n.child_not_ready = n.have_child;
      if (n.parent_pointer) {
        --n.parent_pointer->child_not_ready;
      }
    } else {
      --n.child_not_ready;
    }

    // wait for signal
    if (id != 0) {
      while (n.parent_sense != s) {
        galois::substrate::asmPause();
      }
    }

    // signal children in wakeup tree
    if (leader) {
      if (n.child_pointers[0]) {
        n.child_pointers[0]->parent_sense = s;
      }
      if (n.child_pointers[1]) {
        n.child_pointers[1]->parent_sense = s;
      }
      if (id == 0) {
        n.parent_sense = s;
      }
    }
    ++s;
  }

  const char* name() const override { return "TopoBarrier"; }
};

}  // namespace

std::unique_ptr<galois::substrate::Barrier>
galois::substrate::CreateTopoBarrier(unsigned active_threads) {
  return std::make_unique<TopoBarrier>(active_threads);
}
