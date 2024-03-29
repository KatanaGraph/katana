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

#include "katana/ThreadPool.h"

#include <algorithm>
#include <iostream>

#include "katana/Env.h"
#include "katana/HWTopo.h"
#include "katana/Logging.h"

// Forward declare this to avoid including PerThreadStorage.
// We avoid this to stress that the thread Pool MUST NOT depend on PTS.
namespace katana {

extern void initPTS(unsigned);

}

using katana::ThreadPool;

thread_local ThreadPool::per_signal ThreadPool::my_box;

ThreadPool::ThreadPool()
    : mi(getHWTopo().machineTopoInfo),
      reserved(0),
      masterFastmode(0),
      running(false) {
  signals.resize(mi.maxThreads);
  initThread(0);

  for (unsigned i = 1; i < mi.maxThreads; ++i) {
    std::thread t(&ThreadPool::threadLoop, this, i);
    threads.emplace_back(std::move(t));
  }

  // we don't want signals to have to contain atomics, since they are set once
  while (std::any_of(signals.begin(), signals.end(), [](per_signal* p) {
    return !p || !p->done;
  })) {
    std::atomic_thread_fence(std::memory_order_seq_cst);
  }
}

ThreadPool::~ThreadPool() {
  destroyCommon();
  for (auto& t : threads) {
    t.join();
  }
}

void
ThreadPool::destroyCommon() {
  beKind();  // reset fastmode
  run(mi.maxThreads, []() { throw shutdown_ty(); });
}

void
ThreadPool::burnPower(unsigned num) {
  num = std::min(num, getMaxUsableThreads());

  // changing number of threads?  just do a reset
  if (masterFastmode && masterFastmode != num) {
    beKind();
  }
  if (!masterFastmode) {
    run(num, []() { throw fastmode_ty{true}; });
    masterFastmode = num;
  }
}

void
ThreadPool::beKind() {
  if (masterFastmode) {
    run(masterFastmode, []() { throw fastmode_ty{false}; });
    masterFastmode = 0;
  }
}

// inefficient append
template <typename T>
static void
atomic_append(std::atomic<T*>& headptr, T* newnode) {
  T* n = nullptr;
  if (!headptr.compare_exchange_strong(n, newnode)) {
    atomic_append(headptr.load()->next, newnode);
  }
}

// find id
template <typename T>
static unsigned
findID(std::atomic<T*>& headptr, T* node, unsigned off) {
  T* n = headptr.load();
  KATANA_LOG_DEBUG_ASSERT(n);
  if (n == node) {
    return off;
  }
  return findID(n->next, node, off + 1);
}

template <typename T>
static T*
getNth(std::atomic<T*>& headptr, unsigned off) {
  T* n = headptr.load();
  if (!off) {
    return n;
  }
  return getNth(n->next, off - 1);
}

void
ThreadPool::initThread(unsigned tid) {
  signals[tid] = &my_box;
  my_box.topo = getHWTopo().threadTopoInfo[tid];
  // Initialize
  initPTS(mi.maxThreads);

  if (!GetEnv("KATANA_DO_NOT_BIND_THREADS")) {
    bool bind_main = false;
    if (GetEnv("KATANA_DO_NOT_BIND_MAIN_THREAD")) {
      KATANA_WARN_ONCE(
          "KATANA_DO_NOT_MAIN_THREAD is deprecated.\n"
          "The default behavior is to not bind the main thread.\n"
          "Use KATANA_BIND_MAIN_THREAD to override.");
    }
    if (GetEnv("KATANA_BIND_MAIN_THREAD")) {
      bind_main = true;
    }

    if (my_box.topo.tid != 0 || bind_main) {
      bindThreadSelf(my_box.topo.osContext);
    }
  }
  my_box.done = 1;
}

void
ThreadPool::threadLoop(unsigned tid) {
  initThread(tid);
  bool fastmode = false;
  auto& me = my_box;
  do {
    me.wait(fastmode);
    cascade(fastmode);
    try {
      work();
    } catch (const shutdown_ty&) {
      return;
    } catch (const fastmode_ty& fm) {
      fastmode = fm.mode;
    } catch (const dedicated_ty dt) {
      me.done = 1;
      dt.fn();
      return;
    } catch (const std::exception& exc) {
      // catch anything thrown within try block that derives from std::exception
      std::cerr << exc.what();
      abort();
    } catch (...) {
      abort();
    }
    decascade();
  } while (true);
}

void
ThreadPool::decascade() {
  auto& me = my_box;
  // nothing to wake up
  if (me.wbegin != me.wend) {
    auto midpoint = me.wbegin + (1 + me.wend - me.wbegin) / 2;
    auto& c1done = signals[me.wbegin]->done;
    while (!c1done) {
      asmPause();
    }
    if (midpoint < me.wend) {
      auto& c2done = signals[midpoint]->done;
      while (!c2done) {
        asmPause();
      }
    }
  }
  me.done = 1;
}

void
ThreadPool::cascade(bool fastmode) {
  auto& me = my_box;
  KATANA_LOG_DEBUG_ASSERT(me.wbegin <= me.wend);

  // nothing to wake up
  if (me.wbegin == me.wend) {
    return;
  }

  auto midpoint = me.wbegin + (1 + me.wend - me.wbegin) / 2;

  auto* child1 = signals[me.wbegin];
  child1->wbegin = me.wbegin + 1;
  child1->wend = midpoint;
  child1->wakeup(fastmode);

  if (midpoint < me.wend) {
    auto* child2 = signals[midpoint];
    child2->wbegin = midpoint + 1;
    child2->wend = me.wend;
    child2->wakeup(fastmode);
  }
}

void
ThreadPool::runInternal(unsigned num) {
  // sanitize num
  // seq write to starting should make work safe
  KATANA_LOG_VASSERT(!running, "Recursive thread pool execution not supported");
  running = true;
  num = std::min(std::max(1U, num), getMaxUsableThreads());
  // my_box is tid 0
  auto& me = my_box;
  me.wbegin = 1;
  me.wend = num;

  KATANA_LOG_VASSERT(
      !masterFastmode || masterFastmode == num,
      "fastmode threads {} != num threads {}", masterFastmode, num);
  // launch threads
  cascade(masterFastmode);
  // Do master thread work
  try {
    work();
  } catch (const shutdown_ty&) {
    return;
  } catch (const fastmode_ty& fm) {
  }
  // wait for children
  decascade();
  // Clean up
  work = nullptr;
  running = false;
}

void
ThreadPool::runDedicated(std::function<void(void)>& f) {
  // TODO(ddn): update katana::activeThreads to reflect the dedicated
  // thread but we don't want to depend on katana symbols and too many
  // clients access katana::activeThreads directly.
  KATANA_LOG_VASSERT(
      !running, "Can't start dedicated thread during parallel section");
  ++reserved;

  KATANA_LOG_VASSERT(reserved < mi.maxThreads, "Too many dedicated threads");
  work = [&f]() { throw dedicated_ty{f}; };
  auto* child = signals[mi.maxThreads - reserved];
  child->wbegin = 0;
  child->wend = 0;
  child->done = 0;
  child->wakeup(masterFastmode);
  while (!child->done) {
    asmPause();
  }
  work = nullptr;
}

static katana::ThreadPool* TPOOL = nullptr;

void
katana::internal::SetThreadPool(ThreadPool* tp) {
  KATANA_LOG_VASSERT(!(TPOOL && tp), "Double initialization of ThreadPool");
  TPOOL = tp;
}

katana::ThreadPool&
katana::GetThreadPool() {
  KATANA_LOG_VASSERT(TPOOL, "ThreadPool not initialized");
  return *TPOOL;
}
