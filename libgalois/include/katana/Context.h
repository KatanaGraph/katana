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

#ifndef KATANA_LIBGALOIS_KATANA_CONTEXT_H_
#define KATANA_LIBGALOIS_KATANA_CONTEXT_H_

#include <cassert>
#include <cstdlib>

#include <boost/utility.hpp>

#include "katana/config.h"

#ifdef KATANA_USE_LONGJMP_ABORT
#include <csetjmp>
#endif

#include "katana/MethodFlags.h"
#include "katana/PtrLock.h"
#include "katana/gIO.h"

namespace katana {

enum ConflictFlag {
  CONFLICT = -1,
  NO_CONFLICT = 0,
  REACHED_FAILSAFE = 1,
  BREAK = 2
};

extern thread_local std::jmp_buf execFrame;

class Lockable;

[[noreturn]] inline void
signalConflict(Lockable* = nullptr) {
#if defined(KATANA_USE_LONGJMP_ABORT)
  std::longjmp(execFrame, CONFLICT);
  std::abort();  // shouldn't reach here after longjmp
#elif defined(KATANA_USE_EXCEPTION_ABORT)
  throw CONFLICT;
#endif
}

[[noreturn]] inline void
signalFailSafe(void) {
#if defined(KATANA_USE_LONGJMP_ABORT)
  std::longjmp(katana::execFrame, katana::REACHED_FAILSAFE);
  std::abort();  // shouldn't reach here after longjmp
#elif defined(KATANA_USE_EXCEPTION_ABORT)
  throw REACHED_FAILSAFE;
#endif
}

//! used to release lock over exception path
static inline void
clearConflictLock() {}

class LockManagerBase;

/**
 * All objects that may be locked (nodes primarily) must inherit from
 * Lockable.
 */
class KATANA_EXPORT Lockable {
  PtrLock<LockManagerBase> owner;
  //! Use an intrusive list to track neighborhood of a context without
  //! allocation overhead. Works for cases where a Lockable needs to be only in
  //! one context's neighborhood list
  Lockable* next;
  friend class LockManagerBase;
  friend class SimpleRuntimeContext;

public:
  Lockable() : next(0) {}
};

class KATANA_EXPORT LockManagerBase : private boost::noncopyable {
protected:
  enum AcquireStatus { FAIL, NEW_OWNER, ALREADY_OWNER };

  AcquireStatus tryAcquire(Lockable* lockable);

  inline bool stealByCAS(Lockable* lockable, LockManagerBase* other) {
    assert(lockable != nullptr);
    return lockable->owner.stealing_CAS(other, this);
  }

  inline bool CASowner(Lockable* lockable, LockManagerBase* other) {
    assert(lockable != nullptr);
    return lockable->owner.CAS(other, this);
  }

  inline void setOwner(Lockable* lockable) {
    assert(lockable != nullptr);
    assert(!lockable->owner.getValue());
    lockable->owner.setValue(this);
  }

  inline void release(Lockable* lockable) {
    assert(lockable != nullptr);
    assert(getOwner(lockable) == this);
    lockable->owner.unlock_and_clear();
  }

  inline static bool tryLock(Lockable* lockable) {
    assert(lockable != nullptr);
    return lockable->owner.try_lock();
  }

  inline static LockManagerBase* getOwner(Lockable* lockable) {
    assert(lockable != nullptr);
    return lockable->owner.getValue();
  }
};

class KATANA_EXPORT SimpleRuntimeContext : public LockManagerBase {
  //! The locks we hold
  Lockable* locks;
  bool customAcquire;

protected:
  friend void doAcquire(Lockable*, katana::MethodFlag);

  static SimpleRuntimeContext* getOwner(Lockable* lockable) {
    LockManagerBase* owner = LockManagerBase::getOwner(lockable);
    return static_cast<SimpleRuntimeContext*>(owner);
  }

  virtual void subAcquire(Lockable* lockable, katana::MethodFlag m);

  void addToNhood(Lockable* lockable) {
    assert(!lockable->next);
    lockable->next = locks;
    locks = lockable;
  }

  void acquire(Lockable* lockable, katana::MethodFlag m) {
    AcquireStatus i;
    if (customAcquire) {
      subAcquire(lockable, m);
    } else if ((i = tryAcquire(lockable)) != AcquireStatus::FAIL) {
      if (i == AcquireStatus::NEW_OWNER) {
        addToNhood(lockable);
      }
    } else {
      signalConflict(lockable);
    }
  }

  void release(Lockable* lockable);

public:
  SimpleRuntimeContext(bool child = false) : locks(0), customAcquire(child) {}
  virtual ~SimpleRuntimeContext() {}

  void startIteration() { assert(!locks); }

  unsigned cancelIteration();
  unsigned commitIteration();
};

//! get the current conflict detection class, may be null if not in parallel
//! region
KATANA_EXPORT SimpleRuntimeContext* getThreadContext();

//! used by the parallel code to set up conflict detection per thread
KATANA_EXPORT void setThreadContext(SimpleRuntimeContext* n);

//! Helper function to decide if the conflict detection lock should be taken
inline bool
shouldLock(const katana::MethodFlag g) {
  // Mask out additional "optional" flags
  switch (g & katana::MethodFlag::INTERNAL_MASK) {
  case MethodFlag::UNPROTECTED:
  case MethodFlag::PREVIOUS:
    return false;

  case MethodFlag::READ:
  case MethodFlag::WRITE:
    return true;

  default:
    // Adding error checking code here either upsets the inlining heuristics or
    // icache behavior. Avoid complex code if possible.
    assert(false);
  }
  return false;
}

//! actual locking function.  Will always lock.
inline void
doAcquire(Lockable* lockable, katana::MethodFlag m) {
  SimpleRuntimeContext* ctx = getThreadContext();
  if (ctx)
    ctx->acquire(lockable, m);
}

//! Master function which handles conflict detection
//! used to acquire a lockable thing
inline void
acquire(Lockable* lockable, katana::MethodFlag m) {
  if (shouldLock(m))
    doAcquire(lockable, m);
}

struct AlwaysLockObj {
  void operator()(Lockable* lockable) const {
    doAcquire(lockable, katana::MethodFlag::WRITE);
  }
};

struct CheckedLockObj {
  katana::MethodFlag m;
  CheckedLockObj(katana::MethodFlag _m) : m(_m) {}
  void operator()(Lockable* lockable) const { acquire(lockable, m); }
};

}  // end namespace katana

#endif
