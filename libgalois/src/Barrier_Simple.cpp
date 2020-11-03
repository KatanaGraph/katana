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

#include <condition_variable>
#include <mutex>

#include "galois/substrate/Barrier.h"
#include "galois/substrate/ThreadPool.h"

namespace {

class OneWayBarrier : public galois::substrate::Barrier {
  std::mutex lock;
  std::condition_variable cond;
  unsigned count;
  unsigned total;

public:
  OneWayBarrier(unsigned p) { Reinit(p); }

  void Reinit(unsigned val) override {
    count = 0;
    total = val;
  }

  void Wait() override {
    std::unique_lock<std::mutex> tmp(lock);
    count += 1;
    cond.wait(tmp, [this]() { return count >= total; });
    cond.notify_all();
  }

  const char* name() const override { return "OneWayBarrier"; }
};

class SimpleBarrier : public galois::substrate::Barrier {
  OneWayBarrier barrier1;
  OneWayBarrier barrier2;
  unsigned total;

public:
  SimpleBarrier(unsigned p) : barrier1(p), barrier2(p), total(p) {}

  void Reinit(unsigned val) override {
    total = val;
    barrier1.Reinit(val);
    barrier2.Reinit(val);
  }

  void Wait() override {
    barrier1.Wait();
    if (galois::substrate::ThreadPool::getTID() == 0) {
      barrier1.Reinit(total);
    }
    barrier2.Wait();
    if (galois::substrate::ThreadPool::getTID() == 0) {
      barrier2.Reinit(total);
    }
  }

  const char* name() const override { return "SimpleBarrier"; }
};

}  // end anonymous namespace

std::unique_ptr<galois::substrate::Barrier>
galois::substrate::CreateSimpleBarrier(unsigned active_threads) {
  return std::make_unique<SimpleBarrier>(active_threads);
}
