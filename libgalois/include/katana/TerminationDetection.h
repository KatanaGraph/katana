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

#ifndef GALOIS_LIBGALOIS_GALOIS_SUBSTRATE_TERMINATIONDETECTION_H_
#define GALOIS_LIBGALOIS_GALOIS_SUBSTRATE_TERMINATIONDETECTION_H_

#include <atomic>

#include "galois/config.h"
#include "galois/substrate/CacheLineStorage.h"
#include "galois/substrate/PerThreadStorage.h"

namespace galois::substrate {

class TerminationDetection;

/*
 * Returns the termination detection instance. The instance will be reused, but
 * reinitialized to activeThreads.
 */
GALOIS_EXPORT TerminationDetection& GetTerminationDetection(
    unsigned active_threads);

/// Termination detection is the process of determining whether multiple
/// threads can safely stop executing because no worker has done any
/// work.
///
/// If all workers have not done any work it is safe to finish; otherwise, some
/// worker has done work and thus all workers must continue working.
///
/// The most straightforward way to implement termination detection would be
/// with with an all-reduce operation, which implies a global barrier. Most
/// implementations of termination detection provide a more efficient
/// asynchronous implementation that allows for threads to continue working if
/// possible rather than waiting in a barrier.
///
/// The typical way to use termination detection is:
///
///   // On each thread....
///   TerminationDetection& term = GetTerminationDetection();
///
///   term.InitializeThread();
///
///   do {
///     state = ExamineCurrentState();
///     next_state = ProduceNextState();
///
///     bool did_work = state != next_state;
///
///     term.SignalWorked(did_work);
///
///   } while (term.Working());
///
class GALOIS_EXPORT TerminationDetection {
  // So that GetTerminationDetection can call init.
  friend TerminationDetection& GetTerminationDetection(unsigned);

  CacheLineStorage<std::atomic<int>> global_term_;

protected:
  void SetTerminated() { global_term_ = true; }

  void ResetTerminated() { global_term_ = false; }

  virtual void Init(unsigned active_threads) = 0;

public:
  TerminationDetection() = default;
  virtual ~TerminationDetection();
  TerminationDetection(const TerminationDetection&) = delete;
  TerminationDetection& operator=(const TerminationDetection&) = delete;
  TerminationDetection(TerminationDetection&&) = delete;
  TerminationDetection& operator=(TerminationDetection&&) = delete;

  /// Initializes the per-thread state.  All threads must call this before any
  /// call SignalLocalTerminated.
  virtual void InitializeThread() = 0;

  /// SignalWorked, when work_happened is true, indicates that since the last
  /// time this was called some progress was made that should prevent
  /// termination. When work_happened is false, this indicates that this thread
  /// is ready to terminate.
  ///
  /// This call is thread-safe.
  ///
  /// All threads must call InitializeThread() before any thread calls this
  /// function.  This function should not be on the fast path (this is why it
  /// takes a flag, to allow the caller to buffer up work status changes).
  virtual void SignalWorked(bool work_happened) = 0;

  /// Working returns false iff all threads should terminate
  bool Working() const { return !global_term_.data; }
};

namespace internal {
void SetTerminationDetection(TerminationDetection* term);
}  // end namespace internal

}  // end namespace galois::substrate

#endif
