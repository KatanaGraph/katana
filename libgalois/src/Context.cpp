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

#include "katana/Context.h"

#include <stdio.h>

#include "katana/CacheLineStorage.h"
#include "katana/SimpleLock.h"

//! Global thread context for each active thread
static thread_local katana::SimpleRuntimeContext* thread_ctx = 0;

KATANA_EXPORT thread_local jmp_buf katana::execFrame;

void
katana::setThreadContext(katana::SimpleRuntimeContext* ctx) {
  thread_ctx = ctx;
}

katana::SimpleRuntimeContext*
katana::getThreadContext() {
  return thread_ctx;
}

////////////////////////////////////////////////////////////////////////////////
// LockManagerBase & SimpleRuntimeContext
////////////////////////////////////////////////////////////////////////////////

katana::LockManagerBase::AcquireStatus
katana::LockManagerBase::tryAcquire(katana::Lockable* lockable) {
  assert(lockable);
  if (lockable->owner.try_lock()) {
    lockable->owner.setValue(this);
    return NEW_OWNER;
  } else if (getOwner(lockable) == this) {
    return ALREADY_OWNER;
  }
  return FAIL;
}

void
katana::SimpleRuntimeContext::release(katana::Lockable* lockable) {
  assert(lockable);
  // The deterministic executor, for instance, steals locks from other
  // iterations
  assert(customAcquire || getOwner(lockable) == this);
  assert(!lockable->next);
  lockable->owner.unlock_and_clear();
}

unsigned
katana::SimpleRuntimeContext::commitIteration() {
  unsigned numLocks = 0;
  while (locks) {
    // ORDER MATTERS!
    Lockable* lockable = locks;
    locks = lockable->next;
    lockable->next = 0;
    compilerBarrier();
    release(lockable);
    ++numLocks;
  }

  return numLocks;
}

unsigned
katana::SimpleRuntimeContext::cancelIteration() {
  return commitIteration();
}

void
katana::SimpleRuntimeContext::subAcquire(
    katana::Lockable*, katana::MethodFlag) {
  KATANA_DIE("unreachable");
}
