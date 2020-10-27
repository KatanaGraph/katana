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

#include "galois/substrate/SharedMem.h"

#include <memory>

#include "galois/substrate/Barrier.h"
#include "galois/substrate/PagePool.h"
#include "galois/substrate/Termination.h"
#include "galois/substrate/ThreadPool.h"

struct galois::substrate::SharedMem::Impl {
  struct Dependents {
    internal::LocalTerminationDetection<> term;
    internal::BarrierInstance<> barrier;
    internal::PageAllocState<> page_pool;
  };

  ThreadPool thread_pool;
  std::unique_ptr<Dependents> deps;
};

galois::substrate::SharedMem::SharedMem() : impl_(std::make_unique<Impl>()) {
  internal::setThreadPool(&impl_->thread_pool);

  // The thread pool must be initialized first because other substrate classes
  // may call getThreadPool() in their constructors
  impl_->deps = std::make_unique<Impl::Dependents>();

  internal::setBarrierInstance(&impl_->deps->barrier);
  internal::setTermDetect(&impl_->deps->term);
  internal::setPagePoolState(&impl_->deps->page_pool);
}

galois::substrate::SharedMem::~SharedMem() {
  internal::setPagePoolState(nullptr);
  internal::setTermDetect(nullptr);
  internal::setBarrierInstance(nullptr);

  // Other substrate classes destructors may call getThreadPool() so destroy
  // them first before reseting the thread pool.
  impl_->deps.reset();

  internal::setThreadPool(nullptr);
}
