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

#ifndef KATANA_LIBGALOIS_KATANA_TILEDEXECUTOR_H_
#define KATANA_LIBGALOIS_KATANA_TILEDEXECUTOR_H_

#include "katana/Galois.h"
#include "katana/NUMAArray.h"
#include "katana/NoDerefIterator.h"
#include "katana/config.h"

namespace katana {

template <typename Graph, bool UseExp = false>
class Fixed2DGraphTiledExecutor {
  static constexpr int numDims = 2;  // code is specialized to 2

  using SpinLock = katana::PaddedLock<true>;
  using GNode = typename Graph::GraphNode;
  using iterator = typename Graph::iterator;
  using edge_iterator = typename Graph::edge_iterator;
  using Point = std::array<size_t, numDims>;

  template <typename T>
  struct SimpleAtomic {
    std::atomic<T> value;
    SimpleAtomic() : value(0) {}
    SimpleAtomic(const SimpleAtomic& o) : value(o.value.load()) {}
    T relaxedLoad() { return value.load(std::memory_order_relaxed); }
    void relaxedAdd(T delta) {
      value.store(relaxedLoad() + delta, std::memory_order_relaxed);
    }
  };

  /**
   * Tasks are 2D ranges [startX, endX) x [startY, endYInclusive]
   */
  struct Task {
    iterator startX;
    iterator endX;
    GNode startY;
    GNode endYInclusive;
    Point coord;
    SimpleAtomic<unsigned> updates;
  };

  /**
   * Functor: given a graph on initialization, passing it an edge iterator
   * will return the destination of that edge in the graph.
   */
  struct GetDst {
    Graph* g;
    GetDst() {}
    GetDst(Graph* _g) : g(_g) {}

    GNode operator()(edge_iterator ii) const { return g->getEdgeDst(ii); }
  };

  using no_deref_iterator = katana::NoDerefIterator<edge_iterator>;
  using edge_dst_iterator =
      boost::transform_iterator<GetDst, no_deref_iterator>;

  Graph& g;
  // std::array<katana::NUMAArray<SpinLock>, numDims> locks;
  // katana::NUMAArray<Task> tasks;
  std::array<std::vector<SpinLock>, numDims> locks;
  std::vector<Task> tasks;
  size_t numTasks;
  unsigned maxUpdates;
  bool useLocks;
  katana::GAccumulator<unsigned> failedProbes;

  /**
   * Advance point p in the specified dimension by delta and account for
   * overflow as well.
   *
   * @param p Point to advance
   * @param dim Dimension to advance
   * @param delta Amount to advance by
   */
  void nextPoint(Point& p, int dim, int delta) {
    KATANA_LOG_DEBUG_ASSERT(dim < numDims);
    p[dim] += delta;
    // account for overflow
    while (p[dim] >= locks[dim].size()) {
      p[dim] -= locks[dim].size();
    }
  }

  /**
   * Get task associated with a point in the 2D block grid.
   *
   * For point x, y, get the task indexed into the X direction x times
   * and indexed into the Y direction y times.
   *
   * @param point with coordinates to a task
   * @returns pointer to task associated with the passed in point
   */
  Task* getTask(const Point& p) {
    Task* t = &tasks[p[0] + p[1] * locks[0].size()];

    KATANA_LOG_DEBUG_ASSERT(t < &tasks[numTasks]);
    KATANA_LOG_DEBUG_ASSERT(t >= &tasks[0]);

    return t;
  }

  /**
   * Finds a block starting from the provided point that hasn't reached
   * the maximum number of updates and returns a pointer to it.
   * Uses a lock on each block it probes, and a returned block is **returned
   * with a lock**.
   *
   * @param start Point specifying block to start probe from
   * @param dim Specifies whether or not to continue probe in x (0) or y (1)
   * direction
   * @param n Number of blocks to probe before failing
   * @returns pointer to block that hasn't reached a maximum number of updates
   * or nullptr on probe failure. The block is **returned with the lock**.
   */
  Task* probeBlockWithLock(Point& start, int dim, size_t n) {
    Point p = start;

    for (size_t i = 0; i < n; ++i) {
      Task* t = getTask(p);

      KATANA_LOG_DEBUG_ASSERT(p[0] == t->coord[0]);
      KATANA_LOG_DEBUG_ASSERT(p[1] == t->coord[1]);
      KATANA_LOG_DEBUG_ASSERT(t->coord[0] < locks[0].size());
      KATANA_LOG_DEBUG_ASSERT(t->coord[1] < locks[1].size());

      if (t->updates.relaxedLoad() < maxUpdates) {
        if (std::try_lock(locks[0][t->coord[0]], locks[1][t->coord[1]]) < 0) {
          if (t->updates.relaxedLoad() < maxUpdates) {
            t->updates.relaxedAdd(1);
            start = p;
            return t;
          }

          // TODO add to worklist
          for (int i = 0; i < numDims; ++i) {
            locks[i][t->coord[i]].unlock();
          }
        }
      }

      nextPoint(p, dim, 1);
    }

    failedProbes += 1;
    return nullptr;
  }

  /**
   * Finds a block starting from the provided point that hasn't reached
   * the maximum number of updates and returns a pointer to it.
   *
   * @param start Point specifying block to start probe from
   * @param dim Specifies whether or not to continue probe in x (0) or y (1)
   * direction
   * @param n Number of blocks to probe before failing
   * @returns pointer to block that hasn't reached a maximum number of updates
   * or nullptr on probe failure
   */
  Task* probeBlockWithoutLock(Point& start, int dim, size_t n) {
    Point p = start;

    for (size_t i = 0; i < n; ++i) {
      Task* t = getTask(p);

      KATANA_LOG_DEBUG_ASSERT(p[0] == t->coord[0]);
      KATANA_LOG_DEBUG_ASSERT(p[1] == t->coord[1]);
      KATANA_LOG_DEBUG_ASSERT(t->coord[0] < locks[0].size());
      KATANA_LOG_DEBUG_ASSERT(t->coord[1] < locks[1].size());

      if (t->updates.relaxedLoad() < maxUpdates) {
        if (t->updates.value.fetch_add(1) < maxUpdates) {
          // hasn't reached maxed updates at point of fetch
          start = p;
          return t;
        }
      }
      nextPoint(p, dim, 1);
    }

    failedProbes += 1;
    return nullptr;
  }

  /**
   * Finds a block starting from the provided point that hasn't reached
   * the maximum number of updates (and isn't locked if using locks) and returns
   * a pointer to it. If a task is found, start is updated to the
   * corresponding coordinate.
   *
   * Wrapper for locked and not locked versions.
   * Note that if locks are used, the block return will HAVE THE LOCK for that
   * block.
   *
   * @param start Point specifying block to start probe from
   * @param dim Specifies whether or not to continue probe in x (0) or y (1)
   * direction
   * @param n Number of blocks to probe before failing
   * @returns pointer to block that hasn't reached a maximum number of updates
   * or nullptr on probe failure. If locks are used, the caller will
   * have the lock for the block as well.
   */
  Task* probeBlock(Point& start, int dim, size_t n) {
    KATANA_LOG_DEBUG_ASSERT(dim < 2);

    if (useLocks) {
      return probeBlockWithLock(start, dim, n);
    } else {
      return probeBlockWithoutLock(start, dim, n);
    }
  }

  // TODO (Loc) this function needs an overhaul; right now it's too hacky and
  // imprecise
  /**
   * From the provided start point, find a block that is updateable and return
   * it. Search starts by going up-down left-right from start, but if that
   * fails, begin advancing along the diagonal and searching up-down left-right
   * until the entire grid is traversed without a found block.
   *
   * Updateable = hasn't reached max updates on inspection + isn't locked (if
   * using locks)
   *
   * @param start block to start search from
   * @param inclusive If true, the initial search will include the provided
   * start point as a potential block to look at; otherwise it is COMPLETELY
   * omitted from search (unless you have a non-square grid in which
   * case it might become "extra work"; see TODO below)
   **/
  Task* nextBlock(Point& start, bool inclusive) {
    Task* t;

    // repeats twice just to make sure there are actually no unused blocks
    // TODO this method of termination detection is hacky and imprecise,
    // find a better way
    for (int times = 0; times < 2; ++times) {
      Point limit{{locks[0].size(), locks[1].size()}};

      int inclusiveDelta = (inclusive && times == 0) ? 0 : 1;

      // First iteration (i.e. inclusive = true) is INCLUSIVE of start
      // Otherwise, check the next blocks in the x and y direction for the
      // next block
      for (int i = 0; i < numDims; ++i) {
        Point p = start;
        nextPoint(p, i, inclusiveDelta);

        if ((t = probeBlock(p, i, limit[i] - inclusiveDelta))) {
          start = p;
          return t;
        }
      }

      // if the above for loop failed, it means all blocks in both directions
      // (left->right, up->down) from current block from point are locked
      // and/or all blocks have reached max updates
      Point p = start;
      // solution to above issue in comment = advance using diagonal and check
      // from there
      for (int i = 0; i < numDims; ++i) {
        nextPoint(p, i, 1);
      }

      // below will end up looping through entire grid looking for a block
      // to work on; in some cases a block will be looped over more than once
      // (see below TODO)
      // TODO probably unoptimal: if any limit has hit 0, is it the case that
      // the entire grid has been looked at already? This comment writer thinks
      // the answer is yes in which case the below is doing extra work
      while (std::any_of(
          limit.begin(), limit.end(), [](size_t x) { return x > 0; })) {
        for (int i = 0; i < numDims; ++i) {
          if (limit[i] > 1 && (t = probeBlock(p, i, limit[i] - 1))) {
            start = p;
            return t;
          }
        }

        for (int i = 0; i < numDims; ++i) {
          if (limit[i] > 0) {
            limit[i] -= 1;
            nextPoint(p, i, 1);
          }
        }
      }
    }

    return nullptr;
  }

  /**
   * Apply the provided function to the task/block.
   *
   * Dense update, i.e. update everything in the block even if no edge exists.
   *
   * @tparam UseDense must be true
   * @tparam Function function type
   *
   * @param fn Function to apply to 2 nodes
   * @param task Task that contains block information
   */
  template <bool UseDense, typename Function>
  void executeBlock(
      Function& fn, Task& task, typename std::enable_if<UseDense>::type* = 0) {
    GetDst getDst{&g};

    for (auto ii = task.startX; ii != task.endX; ++ii) {
      for (auto jj = g.begin() + task.startY,
                ej = g.begin() + task.endYInclusive + 1;
           jj != ej; ++jj) {
        fn(*ii, *jj);
      }
    }
  }

  /**
   * Apply the provided function to the task/block.
   *
   * Sparse update, i.e. update nodes only if edge exists.
   *
   * @tparam UseDense must be false
   * @tparam Function function type
   *
   * @param fn Function to apply to 2 nodes + an edge
   * @param task Task that contains block information
   */
  template <bool UseDense, typename Function>
  void executeBlock(
      Function& fn, Task& task, typename std::enable_if<!UseDense>::type* = 0) {
    GetDst getDst{&g};

    for (auto ii = task.startX; ii != task.endX; ++ii) {
      no_deref_iterator nbegin(
          g.edge_begin(*ii, katana::MethodFlag::UNPROTECTED));
      no_deref_iterator nend(g.edge_end(*ii, katana::MethodFlag::UNPROTECTED));

      // iterates over the edges, but edge_dst_iterator xforms it to the dest
      // node itself
      edge_dst_iterator dbegin(nbegin, getDst);
      edge_dst_iterator dend(nend, getDst);

      for (auto jj = std::lower_bound(dbegin, dend, task.startY); jj != dend;) {
        edge_iterator edge = *jj.base();
        if (*jj > task.endYInclusive)
          break;

        fn(*ii, *jj, edge);
        ++jj;
        //}
      }
    }
  }

  // TODO this function is imprecise by virtue of nextBlock being a bad
  // function
  /**
   * Execute a function over the grid. Dynamic work: a thread can potentially
   * get any block.
   *
   * @tparam UseDense dense update (all nodes in block update with all other
   * nodes) or sparse update (update only if edge exists)
   * @tparam Type of function specifying how to do update between nodes
   *
   * @param fn Function used to update 2 nodes
   * @param tid Thread id
   * @param total Total number of threads
   */
  template <bool UseDense, typename Function>
  void executeLoopOrig(Function fn, unsigned tid, unsigned total) {
    Point numBlocks{{locks[0].size(), locks[1].size()}};
    Point block;
    Point start;

    // find out each thread's starting point; essentially what it is doing
    // is assinging each thread to a block on the diagonal to begin with
    for (int i = 0; i < numDims; ++i) {
      block[i] = (numBlocks[i] + total - 1) / total;  // blocks per thread
      start[i] = std::min(block[i] * tid, numBlocks[i] - 1);  // block to start
    }

    unsigned coresPerSocket = katana::GetThreadPool().getMaxCores() /
                              katana::GetThreadPool().getMaxSockets();

    // if using locks, readjust start Y location of this thread to location of
    // the thread's socket
    if (useLocks) {
      start = {
          {start[0], std::min(
                         block[1] * katana::GetThreadPool().getSocket(tid) *
                             coresPerSocket,
                         numBlocks[1] - 1)}};
    }

    Point p = start;

    for (int i = 0;; ++i) {
      Task* t = nextBlock(p, i == 0);
      // TODO: Replace with sparse worklist, etc.
      if (!t)
        break;

      executeBlock<UseDense>(fn, *t);

      // unlock the task block if using locks (next block returns the task with
      // the block locked)
      if (useLocks) {
        for (int i = 0; i < numDims; ++i) {
          locks[i][t->coord[i]].unlock();
        }
      }
    }
  }

  /**
   * Wrapper for calling a loop executor function.
   * @tparam UseDense dense update (all nodes in block update with all other
   * nodes) or sparse update (update only if edge exists)
   * @tparam Type of function specifying how to do update between nodes
   *
   * @param fn Function used to update 2 nodes
   * @param tid Thread id
   * @param total Total number of threads
   */
  template <bool UseDense, typename Function>
  void executeLoop(Function fn, unsigned tid, unsigned total) {
    executeLoopOrig<UseDense>(fn, tid, total);
  }

  /**
   * Given the range of elements in the X dimension and the range of elements
   * in the Y dimension with their respective sizes, divide the grid of
   * work into blocks and save the blocks to this structure.
   *
   * @param firstX first element in X dimension
   * @param lastX last element (non inclusive) in X dimension
   * @param firstY first element in Y dimension
   * @param lastY last element (non inclusive) in Y dimension
   * @param sizeX desired size of blocks in X dimension
   * @param sizeY desired size of blocks in Y dimension
   */
  void initializeTasks(
      iterator firstX, iterator lastX, iterator firstY, iterator lastY,
      size_t sizeX, size_t sizeY) {
    const size_t numXBlocks =
        (std::distance(firstX, lastX) + sizeX - 1) / sizeX;
    const size_t numYBlocks =
        (std::distance(firstY, lastY) + sizeY - 1) / sizeY;
    const size_t numBlocks = numXBlocks * numYBlocks;

    // locks[0].create(numXBlocks);
    // locks[1].create(numYBlocks);
    // tasks.create(numBlocks);
    locks[0].resize(numXBlocks);
    locks[1].resize(numYBlocks);
    tasks.resize(numBlocks);

    // TODO parallelize this?
    // assign each block the X and Y that it is responsible for
    for (size_t i = 0; i < numBlocks; ++i) {
      Task& task = tasks[i];
      task.coord = {{i % numXBlocks, i / numXBlocks}};
      std::tie(task.startX, task.endX) =
          katana::block_range(firstX, lastX, task.coord[0], numXBlocks);
      iterator s;
      iterator e;
      std::tie(s, e) =
          katana::block_range(firstY, lastY, task.coord[1], numYBlocks);
      // Works for CSR graphs
      task.startY = *s;
      task.endYInclusive = *e - 1;
    }
  }

  /**
   * Process assigned to each thread. Each thread calls execute loop which will
   * run the provided function over the grid.
   *
   * @tparam UseDense dense update (all nodes in block update with all other
   * nodes) or sparse update (update only if edge exists)
   * @tparam Function function type
   */
  template <bool UseDense, typename Function>
  struct Process {
    Fixed2DGraphTiledExecutor* self;
    Function fn;

    void operator()(unsigned tid, unsigned total) {
      self->executeLoop<UseDense>(fn, tid, total);
    }
  };

public:
  Fixed2DGraphTiledExecutor(Graph& g) : g(g) {}

  /**
   * Report the number of probe block failures to statistics.
   */
  ~Fixed2DGraphTiledExecutor() {
    katana::ReportStatSingle(
        "TiledExecutor", "ProbeFailures", failedProbes.reduce());
  }

  /**
   * Execute a function on a provided X set of nodes and Y set of nodes
   * for a certain number of iterations. Only update nodes x and y if
   * an edge exists between them (sparse).
   *
   * @tparam Function function type
   *
   * @param firstX first element in X dimension
   * @param lastX last element (non inclusive) in X dimension
   * @param firstY first element in Y dimension
   * @param lastY last element (non inclusive) in Y dimension
   * @param sizeX desired size of blocks in X dimension
   * @param sizeY desired size of blocks in Y dimension
   * @param fn Function used to update nodes
   * @param _useLocks true if locks are desired when updating blocks
   * @param numIterations Max number of iterations to run each block in the
   * tiled executor for
   */
  template <typename Function>
  void execute(
      iterator firstX, iterator lastX, iterator firstY, iterator lastY,
      size_t sizeX, size_t sizeY, Function fn, bool _useLocks,
      unsigned numIterations = 1) {
    initializeTasks(firstX, lastX, firstY, lastY, sizeX, sizeY);
    numTasks = tasks.size();
    maxUpdates = numIterations;
    useLocks = _useLocks;

    Process<false, Function> p{this, fn};

    katana::on_each(p);

    // TODO remove after worklist fix
    if (std::any_of(tasks.begin(), tasks.end(), [this](Task& t) {
          return t.updates.value < maxUpdates;
        })) {
      katana::gWarn("Missing tasks");
    }
  }

  /**
   * Execute a function on a provided X set of nodes and Y set of nodes
   * for a certain number of iterations. Updates nodes x and y regardless
   * of whether or not an edge exists between them (dense).
   *
   * @tparam Function function type
   *
   * @param firstX first element in X dimension
   * @param lastX last element (non inclusive) in X dimension
   * @param firstY first element in Y dimension
   * @param lastY last element (non inclusive) in Y dimension
   * @param sizeX desired size of blocks in X dimension
   * @param sizeY desired size of blocks in Y dimension
   * @param fn Function used to update nodes
   * @param _useLocks true if locks are desired when updating blocks
   * @param numIterations Max number of iterations to run each block in the
   * tiled executor for
   */
  template <typename Function>
  void executeDense(
      iterator firstX, iterator lastX, iterator firstY, iterator lastY,
      size_t sizeX, size_t sizeY, Function fn, bool _useLocks,
      int numIterations = 1) {
    initializeTasks(firstX, lastX, firstY, lastY, sizeX, sizeY);
    numTasks = tasks.size();
    maxUpdates = numIterations;
    useLocks = _useLocks;
    Process<true, Function> p{this, fn};
    katana::on_each(p);

    // TODO remove after worklist fix
    if (std::any_of(tasks.begin(), tasks.end(), [this](Task& t) {
          return t.updates.value < maxUpdates;
        })) {
      katana::gWarn("Missing tasks");
    }
  }
};

}  // namespace katana
#endif
