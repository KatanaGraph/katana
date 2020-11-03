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

#include <memory>

#include "galois/config.h"

namespace galois::substrate {

class GALOIS_EXPORT Barrier {
public:
  Barrier() = default;
  virtual ~Barrier();
  Barrier(const Barrier&) = delete;
  Barrier& operator=(const Barrier&) = delete;
  Barrier(Barrier&&) = delete;
  Barrier& operator=(Barrier&&) = delete;

  // not safe if any thread is in wait
  virtual void Reinit(unsigned val) = 0;

  // Wait at this barrier
  virtual void Wait() = 0;

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
 * happen, use {@link CreateSimpleBarrier()} instead.
 */
GALOIS_EXPORT Barrier& GetBarrier(unsigned active_threads);

/**
 * Create specific types of barriers.  For benchmarking only.  Use
 * GetBarrier() for all production code
 */
GALOIS_EXPORT std::unique_ptr<Barrier> CreatePthreadBarrier(unsigned);
GALOIS_EXPORT std::unique_ptr<Barrier> CreateMCSBarrier(unsigned);
GALOIS_EXPORT std::unique_ptr<Barrier> CreateTopoBarrier(unsigned);
GALOIS_EXPORT std::unique_ptr<Barrier> CreateCountingBarrier(unsigned);
GALOIS_EXPORT std::unique_ptr<Barrier> CreateDisseminationBarrier(unsigned);

/**
 * Creates a new simple barrier. This barrier is not designed to be fast but
 * does guarantee that all threads have left the barrier before returning
 * control. Useful when the number of active threads is modified to avoid a
 * race in GetBarrier().  Client is responsible for deallocating returned
 * barrier.
 */
GALOIS_EXPORT std::unique_ptr<Barrier> CreateSimpleBarrier(unsigned);

namespace internal {

void SetBarrier(Barrier* barrier);

}  // namespace internal

}  // namespace galois::substrate

#endif
