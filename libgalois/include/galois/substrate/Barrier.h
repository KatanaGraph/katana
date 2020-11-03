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

#ifndef GALOIS_LIBGALOIS_GALOIS_SUBSTRATE_BARRIER_H_
#define GALOIS_LIBGALOIS_GALOIS_SUBSTRATE_BARRIER_H_

#include <functional>
#include <memory>

#include "galois/config.h"
#include "galois/gIO.h"
#include "galois/substrate/ThreadPool.h"

namespace galois::substrate {

class GALOIS_EXPORT Barrier {
public:
  virtual ~Barrier();

  // not safe if any thread is in wait
  virtual void reinit(unsigned val) = 0;

  // Wait at this barrier
  virtual void wait() = 0;

  // wait at this barrier
  void operator()() { wait(); }

  // barrier type.
  virtual const char* name() const = 0;
};

/**
 * Return a reference to system barrier.
 *
 * Have a pre-instantiated barrier available for use.
 * This is initialized to the current activeThreads. This barrier
 * is designed to be fast and should be used in the common
 * case.
 *
 * However, there is a race if the number of active threads
 * is modified after using this barrier: some threads may still
 * be in the barrier while the main thread reinitializes this
 * barrier to the new number of active threads. If that may
 * happen, use {@link createSimpleBarrier()} instead.
 */
GALOIS_EXPORT Barrier& getBarrier(unsigned activeThreads);

/**
 * Create specific types of barriers.  For benchmarking only.  Use
 * getBarrier() for all production code
 */
GALOIS_EXPORT std::unique_ptr<Barrier> createPthreadBarrier(unsigned);
GALOIS_EXPORT std::unique_ptr<Barrier> createMCSBarrier(unsigned);
GALOIS_EXPORT std::unique_ptr<Barrier> createTopoBarrier(unsigned);
GALOIS_EXPORT std::unique_ptr<Barrier> createCountingBarrier(unsigned);
GALOIS_EXPORT std::unique_ptr<Barrier> createDisseminationBarrier(unsigned);

/**
 * Creates a new simple barrier. This barrier is not designed to be fast but
 * does gaurantee that all threads have left the barrier before returning
 * control. Useful when the number of active threads is modified to avoid a
 * race in {@link getBarrier()}.  Client is reponsible for deallocating
 * returned barrier.
 */
GALOIS_EXPORT std::unique_ptr<Barrier> createSimpleBarrier(unsigned int);

namespace internal {

template <typename _UNUSED = void>
struct BarrierInstance {
  unsigned m_num_threads;
  std::unique_ptr<Barrier> m_barrier;

  BarrierInstance() {
    m_num_threads = getThreadPool().getMaxThreads();
    m_barrier = createTopoBarrier(m_num_threads);
  }

  Barrier& get(unsigned numT) {
    GALOIS_ASSERT(
        numT > 0, "substrate::getBarrier() number of threads must be > 0");

    numT = std::min(numT, getThreadPool().getMaxUsableThreads());
    numT = std::max(numT, 1U);

    if (numT != m_num_threads) {
      m_num_threads = numT;
      m_barrier->reinit(numT);
    }

    return *m_barrier;
  }
};

void setBarrierInstance(BarrierInstance<>* bi);

}  // end namespace internal

}  // end namespace galois::substrate

#endif
