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

#include "katana/GaloisRT.h"

#include <memory>

#include "katana/Barrier.h"
#include "katana/PagePool.h"
#include "katana/Statistics.h"
#include "katana/TerminationDetection.h"
#include "katana/ThreadPool.h"

namespace {
// Dijkstra style 2-pass ring termination detection
class LocalTerminationDetection : public katana::TerminationDetection {
  struct TokenHolder {
    std::atomic<long> token_is_black;
    std::atomic<long> has_token;
    long process_is_black;
    bool last_was_white;  // only used by the master
  };

  katana::PerThreadStorage<TokenHolder> data_;

  unsigned active_threads_;

  // send token onwards
  void PropToken(bool is_black) {
    unsigned id = katana::ThreadPool::getTID();
    TokenHolder& th = *data_.getRemote((id + 1) % active_threads_);
    th.token_is_black = is_black;
    th.has_token = true;
  }

  bool IsSysMaster() const { return katana::ThreadPool::getTID() == 0; }

protected:
  void Init(unsigned active_threads) override {
    active_threads_ = active_threads;
  }

public:
  void InitializeThread() override {
    TokenHolder& th = *data_.getLocal();
    th.token_is_black = false;
    th.process_is_black = true;
    th.last_was_white = true;
    ResetTerminated();
    if (IsSysMaster())
      th.has_token = true;
    else
      th.has_token = false;
  }

  void SignalWorked(bool work_happened) override {
    KATANA_LOG_DEBUG_ASSERT(!(work_happened && !Working()));
    TokenHolder& th = *data_.getLocal();
    th.process_is_black |= work_happened;
    if (th.has_token) {
      if (IsSysMaster()) {
        bool failed = th.token_is_black || th.process_is_black;
        th.token_is_black = th.process_is_black = false;
        if (th.last_was_white && !failed) {
          // This was the second success
          SetTerminated();
          return;
        }
        th.last_was_white = !failed;
      }
      // Normal thread or recirc by master
      KATANA_LOG_DEBUG_VASSERT(
          Working(), "no token should be in progress after globalTerm");
      bool taint = th.process_is_black || th.token_is_black;
      th.process_is_black = th.token_is_black = false;
      th.has_token = false;
      PropToken(taint);
    }
  }
};

// Dijkstra style 2-pass tree termination detection
class TreeTerminationDetection : public katana::TerminationDetection {
  static constexpr int kNumChildren = 2;

  struct TokenHolder {
    // incoming from above
    volatile long down_token;
    // incoming from below
    volatile long up_token[kNumChildren];
    // my state
    long process_is_black;
    bool has_token;
    bool last_was_white;  // only used by the master
    int parent;
    int parent_offset;
    TokenHolder* child[kNumChildren];
  };

  katana::PerThreadStorage<TokenHolder> data_;

  unsigned active_threads_;

  void ProcessToken() {
    TokenHolder& th = *data_.getLocal();
    // int myid = LL::getTID();
    // have all up tokens?
    bool have_all = th.has_token;
    bool black = th.process_is_black;
    for (int i = 0; i < kNumChildren; ++i) {
      if (th.child[i]) {
        if (th.up_token[i] == -1) {
          have_all = false;
        } else {
          black |= th.up_token[i];
        }
      }
    }
    // Have the tokens, propagate
    if (have_all) {
      th.process_is_black = false;
      th.has_token = false;
      if (IsSysMaster()) {
        if (th.last_was_white && !black) {
          // This was the second success
          SetTerminated();
          return;
        }
        th.last_was_white = !black;
        th.down_token = true;
      } else {
        data_.getRemote(th.parent)->up_token[th.parent_offset] = black;
      }
    }

    // received a down token, propagate
    if (th.down_token) {
      th.down_token = false;
      th.has_token = true;
      for (int i = 0; i < kNumChildren; ++i) {
        th.up_token[i] = -1;
        if (th.child[i])
          th.child[i]->down_token = true;
      }
    }
  }

  bool IsSysMaster() const { return katana::ThreadPool::getTID() == 0; }

protected:
  void Init(unsigned active_threads) override {
    active_threads_ = active_threads;
  }

public:
  void InitializeThread() override {
    TokenHolder& th = *data_.getLocal();
    th.down_token = false;
    for (int i = 0; i < kNumChildren; ++i) {
      th.up_token[i] = false;
    }
    th.process_is_black = true;
    th.has_token = false;
    th.last_was_white = false;
    ResetTerminated();
    auto tid = katana::ThreadPool::getTID();
    th.parent = (tid - 1) / kNumChildren;
    th.parent_offset = (tid - 1) % kNumChildren;
    for (unsigned i = 0; i < kNumChildren; ++i) {
      unsigned cn = tid * kNumChildren + i + 1;
      if (cn < active_threads_) {
        th.child[i] = data_.getRemote(cn);
      } else {
        th.child[i] = 0;
      }
    }
    if (IsSysMaster()) {
      th.down_token = true;
    }
  }

  void SignalWorked(bool work_happened) override {
    KATANA_LOG_DEBUG_ASSERT(!(work_happened && !Working()));
    TokenHolder& th = *data_.getLocal();
    th.process_is_black |= work_happened;
    ProcessToken();
  }
};

}  // namespace

struct katana::GaloisRT::Impl {
  struct Dependents {
    LocalTerminationDetection term;
    std::unique_ptr<Barrier> barrier;
    internal::PageAllocState<> page_pool;
    katana::StatManager stat_manager;
  };

  ThreadPool thread_pool;
  std::unique_ptr<Dependents> deps;
};

katana::GaloisRT::GaloisRT() : impl_(std::make_unique<Impl>()) {
  internal::SetThreadPool(&impl_->thread_pool);

  // The thread pool must be initialized first because other substrate classes
  // may call GetThreadPool() in their constructors
  impl_->deps = std::make_unique<Impl::Dependents>();
  impl_->deps->barrier =
      katana::CreateTopoBarrier(impl_->thread_pool.getMaxUsableThreads());

  internal::SetBarrier(impl_->deps->barrier.get());
  internal::SetTerminationDetection(&impl_->deps->term);
  internal::setPagePoolState(&impl_->deps->page_pool);
  katana::internal::setSysStatManager(&impl_->deps->stat_manager);
}

katana::GaloisRT::~GaloisRT() {
  katana::PrintStats();
  katana::internal::setSysStatManager(nullptr);
  internal::setPagePoolState(nullptr);
  internal::SetTerminationDetection(nullptr);
  internal::SetBarrier(nullptr);

  // Other substrate classes destructors may call GetThreadPool() so destroy
  // them first before reseting the thread pool.
  impl_->deps.reset();

  internal::SetThreadPool(nullptr);
}
