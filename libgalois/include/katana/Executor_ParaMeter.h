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

#ifndef KATANA_LIBGALOIS_KATANA_EXECUTORPARAMETER_H_
#define KATANA_LIBGALOIS_KATANA_EXECUTORPARAMETER_H_

#include <algorithm>
#include <cstdio>
#include <cstdlib>
#include <ctime>
#include <deque>
#include <random>
#include <vector>

#include "katana/Context.h"
#include "katana/Executor_DoAll.h"
#include "katana/Executor_ForEach.h"
#include "katana/Executor_OnEach.h"
#include "katana/Mem.h"
#include "katana/Reduction.h"
#include "katana/Simple.h"
#include "katana/Traits.h"
#include "katana/config.h"
#include "katana/gIO.h"

namespace katana {

namespace parameter {

struct StepStatsBase {
  static inline void printHeader(FILE* out) {
    fprintf(
        out, "LOOPNAME, STEP, PARALLELISM, WORKLIST_SIZE, NEIGHBORHOOD_SIZE\n");
  }

  static inline void dump(
      FILE* out, const char* loopname, size_t step, size_t parallelism,
      size_t wlSize, size_t nhSize) {
    assert(out && "StepStatsBase::dump() file handle is null");
    fprintf(
        out, "%s, %zu, %zu, %zu, %zu\n", loopname, step, parallelism, wlSize,
        nhSize);
  }
};

struct OrderedStepStats : public StepStatsBase {
  using Base = StepStatsBase;

  const size_t step;
  GAccumulator<size_t> parallelism;
  const size_t wlSize;

  explicit OrderedStepStats(size_t _step, size_t _wlsz)
      : Base(), step(_step), parallelism(), wlSize(_wlsz) {}

  explicit OrderedStepStats(size_t _step, size_t par, size_t _wlsz)
      : Base(), step(_step), parallelism(), wlSize(_wlsz) {
    parallelism += par;
  }

  void dump(FILE* out, const char* loopname) {
    Base::dump(out, loopname, step, parallelism.reduce(), wlSize, 0ul);
  }
};

struct UnorderedStepStats : public StepStatsBase {
  using Base = StepStatsBase;

  size_t step;
  GAccumulator<size_t> parallelism;
  GAccumulator<size_t> wlSize;
  GAccumulator<size_t> nhSize;

  UnorderedStepStats() : Base(), step(0) {}

  void nextStep() {
    ++step;
    parallelism.reset();
    wlSize.reset();
    nhSize.reset();
  }

  void dump(FILE* out, const char* loopname) {
    Base::dump(
        out, loopname, step, parallelism.reduce(), wlSize.reduce(),
        nhSize.reduce());
  }
};

// Single ParaMeter stats file per run of an app
// which includes all instances of for_each loops
// run with ParaMeter Executor
KATANA_EXPORT FILE* getStatsFile();
KATANA_EXPORT void closeStatsFile();

template <typename T>
class FIFO_WL {
  using PTcont = katana::PerThreadStorage<katana::gstl::Vector<T>>;

  std::array<PTcont, 2> worklists;

  PTcont* curr;
  PTcont* next;

public:
  FIFO_WL() : curr(&worklists[0]), next(&worklists[1]) {}

  auto iterateCurr() { return katana::MakeLocalTwoLevelRange(*curr); }

  void pushNext(const T& item) { next->getLocal()->push_back(item); }

  void nextStep() {
    std::swap(curr, next);
    katana::on_each_gen(
        [this](const unsigned, const unsigned) { next->getLocal()->clear(); },
        std::make_tuple());
  }

  PTcont* currentWorklist() { return curr; }

  bool empty() const {
    for (unsigned i = 0, n = next->size(); i < n; ++i) {
      if (!next->getRemote(i)->empty()) {
        return false;
      }
    }
    return true;
  }
};

template <typename T>
class RAND_WL : public FIFO_WL<T> {
public:
  auto iterateCurr() {
    katana::on_each_gen(
        [&](int, int) {
          auto& lwl = *this->currentWorklist()->getLocal();

          std::random_device r;
          std::mt19937 rng(r());
          std::shuffle(lwl.begin(), lwl.end(), rng);
        },
        std::make_tuple());

    return MakeLocalTwoLevelRange(*this->currentWorklist());
  }
};

template <typename T>
class LIFO_WL : public FIFO_WL<T> {
public:
  auto iterateCurr() {
    // TODO: use reverse iterator instead of std::reverse
    katana::on_each_gen(
        [&](int, int) {
          auto& lwl = *this->currentWorklist()->getLocal();
          std::reverse(lwl.begin(), lwl.end());
        },
        std::make_tuple());

    return MakeLocalTwoLevelRange(*this->currentWorklist());
  }
};

enum class SchedType { FIFO, RAND, LIFO };

template <typename T, SchedType SCHED>
struct ChooseWL {};

template <typename T>
struct ChooseWL<T, SchedType::FIFO> {
  using type = FIFO_WL<T>;
};

template <typename T>
struct ChooseWL<T, SchedType::LIFO> {
  using type = LIFO_WL<T>;
};

template <typename T>
struct ChooseWL<T, SchedType::RAND> {
  using type = RAND_WL<T>;
};

template <class T, class FunctionTy, class ArgsTy>
class ParaMeterExecutor {
  using value_type = T;
  using GenericWL = typename trait_type<wl_tag, ArgsTy>::type::type;
  using WorkListTy = typename GenericWL::template retype<T>;
  using dbg = katana::debug<1>;

  constexpr static bool needsStats = !has_trait<no_stats_tag, ArgsTy>();
  constexpr static bool needsPush = !has_trait<no_pushes_tag, ArgsTy>();
  constexpr static bool needsAborts =
      !has_trait<disable_conflict_detection_tag, ArgsTy>();
  constexpr static bool needsPia = has_trait<per_iter_alloc_tag, ArgsTy>();
  constexpr static bool needsBreak = has_trait<parallel_break_tag, ArgsTy>();

  struct IterationContext {
    T item;
    bool doabort;
    katana::UserContextAccess<value_type> facing;
    SimpleRuntimeContext ctx;

    explicit IterationContext(const T& v) : item(v), doabort(false) {}

    void reset() {
      doabort = false;
      if (needsPia)
        facing.resetAlloc();

      if (needsPush)
        facing.getPushBuffer().clear();
    }
  };

  using PWL = typename ChooseWL<IterationContext*, WorkListTy::SCHEDULE>::type;

private:
  PWL m_wl;
  FunctionTy m_func;
  const char* loopname;
  FILE* m_statsFile;
  FixedSizeAllocator<IterationContext> m_iterAlloc;
  katana::GReduceLogicalOr m_broken;

  IterationContext* newIteration(const T& item) {
    IterationContext* it = m_iterAlloc.allocate(1);
    assert(it && "IterationContext allocation failed");

    m_iterAlloc.construct(it, item);

    it->reset();
    return it;
  }

  unsigned abortIteration(IterationContext* it) {
    assert(it && "nullptr arg");
    assert(
        it->doabort &&
        "aborting an iteration without setting its doabort flag");

    unsigned numLocks = it->ctx.cancelIteration();
    it->reset();

    m_wl.pushNext(it);
    return numLocks;
  }

  unsigned commitIteration(IterationContext* it) {
    assert(it && "nullptr arg");

    if (needsPush) {
      for (const auto& item : it->facing.getPushBuffer()) {
        IterationContext* child = newIteration(item);
        m_wl.pushNext(child);
      }
    }

    unsigned numLocks = it->ctx.commitIteration();
    it->reset();

    m_iterAlloc.destroy(it);
    m_iterAlloc.deallocate(it, 1);

    return numLocks;
  }

private:
  void runSimpleStep(UnorderedStepStats& stats) {
    katana::do_all_gen(
        m_wl.iterateCurr(),
        [&, this](IterationContext* it) {
          stats.wlSize += 1;

          setThreadContext(&(it->ctx));

          m_func(it->item, it->facing.data());
          stats.parallelism += 1;
          unsigned nh = commitIteration(it);
          stats.nhSize += nh;

          setThreadContext(nullptr);
        },
        std::make_tuple(katana::steal(), katana::loopname("ParaM-Simple")));
  }

  void runCautiousStep(UnorderedStepStats& stats) {
    katana::do_all_gen(
        m_wl.iterateCurr(),
        [&, this](IterationContext* it) {
          stats.wlSize += 1;

          setThreadContext(&(it->ctx));
          bool broke = false;

          if (needsBreak) {
            it->facing.setBreakFlag(&broke);
          }
#ifdef KATANA_USE_LONGJMP_ABORT
          int flag = 0;
          if ((flag = setjmp(execFrame)) == 0) {
            m_func(it->item, it->facing.data());
          } else
#elif KATANA_USE_EXCEPTION_ABORT
          try {
            m_func(it->item, it->facing.data());

          } catch (const ConflictFlag& flag)
#endif
          {
            clearConflictLock();
            switch (flag) {
            case katana::CONFLICT:
              it->doabort = true;
              break;
            default:
              std::abort();
            }
          }

          if (needsBreak && broke) {
            m_broken.update(true);
          }

          setThreadContext(nullptr);
        },
        std::make_tuple(katana::steal(), katana::loopname("ParaM-Expand-NH")));

    katana::do_all_gen(
        m_wl.iterateCurr(),
        [&, this](IterationContext* it) {
          if (it->doabort) {
            abortIteration(it);

          } else {
            stats.parallelism += 1;
            unsigned nh = commitIteration(it);
            stats.nhSize += nh;
          }
        },
        std::make_tuple(katana::steal(), katana::loopname("ParaM-Commit")));
  }

  template <typename R>
  void execute(const R& range) {
    katana::on_each_gen(
        [&, this](const unsigned, const unsigned) {
          auto begin = range.local_begin();
          auto end = range.local_end();

          for (auto i = begin; i != end; ++i) {
            IterationContext* it = newIteration(*i);
            m_wl.pushNext(it);
          }
        },
        std::make_tuple());

    UnorderedStepStats stats;

    while (!m_wl.empty()) {
      m_wl.nextStep();

      if (needsAborts) {
        runCautiousStep(stats);

      } else {
        runSimpleStep(stats);
      }

      // dbg::print("Step: ", stats.step, ", Parallelism: ",
      // stats.parallelism.reduce());
      assert(stats.parallelism.reduce() && "ERROR: No Progress");

      stats.dump(m_statsFile, loopname);
      stats.nextStep();

      if (needsBreak && m_broken.reduce()) {
        break;
      }

    }  // end while

    closeStatsFile();
  }

public:
  ParaMeterExecutor(const FunctionTy& f, const ArgsTy& args)
      : m_func(f),
        loopname(katana::internal::getLoopName(args)),
        m_statsFile(getStatsFile()) {}

  // called serially once
  template <typename RangeTy>
  void init(const RangeTy& range) {
    execute(range);
  }

  // called once on each thread followed by a barrier
  template <typename RangeTy>
  void initThread(const RangeTy&) const {}

  void operator()() {}
};

}  // namespace parameter

template <
    class T = int, parameter::SchedType SCHED = parameter::SchedType::FIFO>
class ParaMeter {
public:
  template <bool _concurrent>
  using rethread = ParaMeter<T, SCHED>;

  template <typename _T>
  using retype = ParaMeter<_T, SCHED>;

  using value_type = T;

  constexpr static const parameter::SchedType SCHEDULE = SCHED;

  using fifo = ParaMeter<T, parameter::SchedType::FIFO>;
  using random = ParaMeter<T, parameter::SchedType::RAND>;
  using lifo = ParaMeter<T, parameter::SchedType::LIFO>;
};

// hookup into katana::for_each. Invoke katana::for_each with
// wl<katana::ParaMeter<> >
template <class T, class FunctionTy, class ArgsTy>
struct ForEachExecutor<katana::ParaMeter<T>, FunctionTy, ArgsTy>
    : public parameter::ParaMeterExecutor<T, FunctionTy, ArgsTy> {
  using SuperTy = parameter::ParaMeterExecutor<T, FunctionTy, ArgsTy>;
  ForEachExecutor(const FunctionTy& f, const ArgsTy& args) : SuperTy(f, args) {}
};

//! invoke ParaMeter tool to execute a for_each style loop
template <typename R, typename F, typename ArgsTuple>
void
for_each_ParaMeter(const R& range, const F& func, const ArgsTuple& argsTuple) {
  using T = typename R::values_type;

  auto tpl = katana::get_default_trait_values(
      argsTuple, std::make_tuple(wl_tag{}),
      std::make_tuple(wl<katana::ParaMeter<>>()));

  using Tpl_ty = decltype(tpl);

  using Exec = parameter::ParaMeterExecutor<T, F, Tpl_ty>;
  Exec exec(func, tpl);

  exec.execute(range);
}

}  // end namespace katana
#endif

/*
 * requirements:
 * - support random and fifo schedules, maybe lifo
 * - write stats to a single file.
 * - support multi-threaded execution
 *
 * interface:
 * - file set by environment variable
 * - ParaMeter invoked by choosing wl type, e.g. ParaMeter<>::with_rand, or
 * ParaMeter<>::fifo
 */
