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

#ifndef KATANA_LIBGALOIS_KATANA_RANGE_H_
#define KATANA_LIBGALOIS_KATANA_RANGE_H_

#include <iterator>
#include <type_traits>

#include <boost/iterator/counting_iterator.hpp>

#include "katana/ThreadPool.h"
#include "katana/TwoLevelIterator.h"
#include "katana/config.h"
#include "katana/gstl.h"

namespace katana {

/**
 * A LocalRange is a range specialized to containers that have a concept of
 * local ranges (i.e., local_begin and local_end).
 *
 * Local ranges partition a container into portions that are local to each
 * thread. The local_begin and local_end methods return the portion local to
 * the current thread.
 */
template <typename T>
class LocalRange {
public:
  typedef typename T::iterator iterator;
  typedef typename T::local_iterator local_iterator;
  typedef typename std::iterator_traits<iterator>::value_type value_type;

  LocalRange(T& c) : container(c) {}

  iterator begin() const { return container.begin(); }
  iterator end() const { return container.end(); }

  local_iterator local_begin() const { return container.local_begin(); }
  local_iterator local_end() const { return container.local_end(); }

private:
  T& container;
};

template <typename T>
LocalRange<T>
MakeLocalRange(T& obj) {
  return LocalRange<T>(obj);
}

/**
 * A LocalTwoLevelRange is a range over a container (outer) of containers
 * (inner) where the outer container has local ranges and the overall range
 * should be over elements of the inner container.
 *
 * This range is commonly used when iterating over thread-local containers.
 * The initial elements should be drawn from the container local to the current
 * thread, but overall iteration space is the sum of all thread-local
 * containers (e.g., for work-stealing).
 */
template <typename T>
class LocalTwoLevelRange {
public:
  typedef typename katana::TwoLevelIterator<typename T::iterator> iterator;
  typedef typename std::iterator_traits<
      typename T::local_iterator>::value_type::iterator local_iterator;
  typedef typename std::iterator_traits<iterator>::value_type value_type;

  LocalTwoLevelRange(T& c) : container(c) {}

  iterator begin() const {
    return make_two_level_iterator(container.begin(), container.end().first);
  }

  iterator end() const {
    return make_two_level_iterator(container.begin(), container.end().second);
  }

  local_iterator local_begin() const { return container.getLocal()->begin(); }

  local_iterator local_end() const { return container.getLocal()->end(); }

private:
  T& container;
};

template <typename T>
LocalTwoLevelRange<T>
MakeLocalTwoLevelRange(T& obj) {
  return LocalTwoLevelRange<T>(obj);
}

/**
 * A StandardRange is a range over standard C++ begin and end iterators.
 */
template <typename IterTy>
class StandardRange {
public:
  typedef IterTy iterator;
  typedef iterator local_iterator;
  typedef typename std::iterator_traits<IterTy>::value_type value_type;

  StandardRange() = default;

  StandardRange(IterTy begin, IterTy end) : begin_(begin), end_(end) {}

  iterator begin() const { return begin_; }
  iterator end() const { return end_; }

  local_iterator local_begin() const { return local_pair().first; }
  local_iterator local_end() const { return local_pair().second; }

private:
  std::pair<local_iterator, local_iterator> local_pair() const {
    return katana::block_range(
        begin_, end_, ThreadPool::getTID(), katana::activeThreads);
  }

  IterTy begin_;
  IterTy end_;
};

template <typename IterTy>
StandardRange<IterTy>
MakeStandardRange(IterTy begin, IterTy end) {
  return StandardRange<IterTy>(begin, end);
}

/**
 * SpecificRange is a range type where a threads range is specified by an int
 * array that gives where each thread should begin its iteration
 */
template <typename IterTy>
class SpecificRange {
  // TODO(ddn): This range is not generalizable to all iterators; probably
  // move into libcusp or provide a specific numeric range class to allow
  // easy construction of local iteration spaces.
public:
  typedef IterTy iterator;
  typedef iterator local_iterator;
  typedef typename std::iterator_traits<IterTy>::value_type value_type;

private:
  /**
   * Using the thread_beginnings array which tells you which node each thread
   * should begin at, we can get the local block range for a particular
   * thread. If the local range falls outside of global range, do nothing.
   *
   * @returns A pair of iterators that specifies the beginning and end
   * of the range for this particular thread.
   */
  std::pair<local_iterator, local_iterator> local_pair() const {
    uint32_t my_thread_id = ThreadPool::getTID();
    uint32_t total_threads = activeThreads;

    iterator local_begin = thread_beginnings_[my_thread_id];
    iterator local_end = thread_beginnings_[my_thread_id + 1];

    assert(local_begin <= local_end);

    if (thread_beginnings_[total_threads] == *global_end_ &&
        *global_begin_ == 0) {
      return std::make_pair(local_begin, local_end);
    }

    // This path assumes that we were passed in thread_beginnings for the
    // range 0 to last node, but the passed in range to execute is NOT the
    // entire 0 to thread end range; therefore, work under the assumption that
    // only some threads will execute things only if they "own" nodes in the
    // range
    iterator left = local_begin;
    iterator right = local_end;

    // local = what this thread CAN do
    // global = what this thread NEEDS to do

    // cutoff left and right if global begin/end require less than what we
    // need
    if (local_begin < global_begin_) {
      left = global_begin_;
    }
    if (local_end > global_end_) {
      right = global_end_;
    }
    // make sure range is sensible after changing left and right
    if (left >= right || right <= left) {
      left = right = global_end_;
    }

    // Explanations/reasoning of possible cases
    // [ ] = local ranges
    // o = need to be included; global ranges = leftmost and rightmost circle
    // x = not included
    // ooooo[ooooooooxxxx]xxxxxx handled (left the same, right moved)
    // xxxxx[xxxxxooooooo]oooooo handled (left moved, right the same)
    // xxxxx[xxoooooooxxx]xxxxxx handled (both left/right moved)
    // xxxxx[xxxxxxxxxxxx]oooooo handled (left will be >= right, set l = r)
    // oooox[xxxxxxxxxxxx]xxxxxx handled (right will be <= left, set l = r)
    // xxxxx[oooooooooooo]xxxxxx handled (left, right the same = local range)

    return std::make_pair(left, right);
  }

  IterTy global_begin_;
  IterTy global_end_;
  std::vector<uint32_t> thread_beginnings_;

public:
  SpecificRange(IterTy begin, IterTy end, std::vector<uint32_t> thread_ranges)
      : global_begin_(begin),
        global_end_(end),
        thread_beginnings_(std::move(thread_ranges)) {}

  iterator begin() const { return global_begin_; }
  iterator end() const { return global_end_; }

  local_iterator local_begin() const { return local_pair().first; }
  local_iterator local_end() const { return local_pair().second; }
};

/**
 * Creates a SpecificRange object.
 *
 * @tparam IterTy The iterator type used by the range object
 * @param begin The global beginning of the range
 * @param end The global end of the range
 * @param thread_ranges An array of iterators that specifies where each
 * thread's range begins
 * @returns A SpecificRange object
 */
template <typename IterTy>
SpecificRange<IterTy>
MakeSpecificRange(
    IterTy begin, IterTy end, const std::vector<uint32_t>& thread_ranges) {
  return SpecificRange<IterTy>(begin, end, thread_ranges);
}

template <typename T>
struct has_local_iterator {
  template <typename U>
  constexpr static decltype(std::declval<U>().local_begin(), bool()) test(int) {
    return true;
  }

  template <typename U>
  constexpr static bool test(...) {
    return false;
  }

  constexpr static bool value = test<T>(0);
};

template <typename T>
constexpr bool has_local_iterator_v = has_local_iterator<T>::value;

/**
 * Iterate returns a specialized range object for various container-like
 * objects:
 *
 * - A standard range: iterate(begin, end)
 * - A standard container: iterator(container)
 * - A container with local iterators: iterator(container)
 * - An initializer list: iterator({1, 2})
 * - A numeric range: iterator(1, 2)
 */

template <
    typename T, typename std::enable_if_t<has_local_iterator_v<T>>* = nullptr>
auto
iterate(T& container) {
  return MakeLocalRange(container);
}

template <
    typename T, typename std::enable_if_t<!has_local_iterator_v<T>>* = nullptr>
auto
iterate(T& container) {
  return MakeStandardRange(container.begin(), container.end());
}

template <typename T>
auto
iterate(std::initializer_list<T> init_list) {
  return MakeStandardRange(init_list.begin(), init_list.end());
}

template <
    typename T, typename std::enable_if_t<std::is_integral_v<T>>* = nullptr>
auto
iterate(const T& begin, const T& end) {
  return MakeStandardRange(
      boost::counting_iterator<T>(begin), boost::counting_iterator<T>(end));
}

template <
    typename T, typename std::enable_if_t<!std::is_integral_v<T>>* = nullptr>
auto
iterate(const T& begin, const T& end) {
  return MakeStandardRange(begin, end);
}

}  // end namespace katana
#endif
