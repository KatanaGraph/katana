#ifndef KATANA_LIBSUPPORT_KATANA_ITERATORS_H_
#define KATANA_LIBSUPPORT_KATANA_ITERATORS_H_

#include <algorithm>
#include <tuple>
#include <utility>

#include <boost/iterator/iterator_facade.hpp>

#include "Logging.h"
#include "katana/config.h"

namespace katana {

//TODO(amber): Move other iterators from libgalois to libsupport

///  A Zip Iterator allows iterating over several containers simultaneously.
///  A canonical example is sorting two containers together. A Zip iterator maintains
///  a tuple of participating iterators and advances all of them together.
///  Dereferencing yields a tuple of references for l-values and a tuple of values
///  for r-values. See  code below:
///  for example usage:
///
///  NOTE: It is best to define comparator functions as template functions (or
///  lambdas that take parameters as 'auto'), e.g., for std::sort(),
///  because either left or right parameter to the comparator can be a
///  tuple of references or tuple of values depending on the calling context.
///
///int main() {
///
///  std::vector<int> vecA{ 5, 3, 4, 2, 1};
///  std::vector<char> vecB{ 'e', 'c', 'd', 'b', 'a' };
///
///  auto beg = katana::make_zip_iterator(vecA.begin(), vecB.begin());
///
///  auto end = katana::make_zip_iterator(vecA.end(), vecB.end());
///
///  std::cout << "[";
///  for (auto i = beg; i != end; ++i) {
///    std::cout << "(" << std::get<0>(*i) << "," << std::get<1>(*i) << "), ";
///  }
///  std::cout << "]" << std::endl;
///
///  std::sort(beg, end,
///      [&] (const auto& tup1, const auto& tup2) {
///        return std::get<0>(tup1) < std::get<0>(tup2);
///      });
///
///
///  std::cout << "[";
///  for (size_t i = 0; i < vecA.size(); ++i) {
///    std::cout << "(" << vecA[i] << "," << vecB[i] << "), ";
///  }
///  std::cout << "]" << std::endl;
///
///  return 0;
///}

/// This is a thin wrapper around std::tuple<Args&...> . It is needed to
/// define the swap operator (needed by things like std::sort()), without having to
/// overload or conflict with std::swap for std::tuple<>
template <typename... Args>
class KATANA_EXPORT ZipRefTuple : public std::tuple<Args&...> {
  using Base = std::tuple<Args&...>;

public:
  using ZipValTuple = std::tuple<std::remove_reference_t<Args>...>;

  // ZipRefTuple(Args&... args): Base(args...) {}
  ZipRefTuple(const Base& t) : Base(t) {}
  ZipRefTuple(Base&& t) : Base(std::move(t)) {}

  ZipRefTuple(const ZipRefTuple& that) = default;
  ZipRefTuple(ZipRefTuple&& that) = default;
  ZipRefTuple& operator=(const ZipRefTuple& that) = default;
  ZipRefTuple& operator=(ZipRefTuple&& that) = default;

  ZipRefTuple& operator=(const ZipValTuple& that) {
    ApplyAssign(*this, that, std::index_sequence_for<Args...>());
    return *this;
  }

  ZipRefTuple& operator=(ZipValTuple&& that) {
    ApplyMoveAssign(*this, that, std::index_sequence_for<Args...>());
    return *this;
  }

  friend void swap(ZipRefTuple a, ZipRefTuple b) noexcept { return a.swap(b); }

private:
  template <typename Tuple1, typename Tuple2, size_t... Indices>
  static void ApplyAssign(
      Tuple1& tup1, const Tuple2& tup2,
      const std::index_sequence<Indices...>&) {
    ((std::get<Indices>(tup1) = std::get<Indices>(tup2)), ...);
  }

  template <typename Tuple1, typename Tuple2, size_t... Indices>
  static void ApplyMoveAssign(
      Tuple1& tup1, Tuple2&& tup2, const std::index_sequence<Indices...>&) {
    ((std::get<Indices>(tup1) = std::move(std::get<Indices>(tup2))), ...);
  }
};

template <typename... Iterators>
class KATANA_EXPORT ZipIterator
    : public boost::iterator_facade<
          ZipIterator<Iterators...>,
          std::tuple<typename std::iterator_traits<Iterators>::value_type...>,
          std::random_access_iterator_tag,
          ZipRefTuple<typename std::iterator_traits<Iterators>::reference...> >

{
  friend class boost::iterator_core_access;

  using IterTuple = std::tuple<Iterators...>;
  using RefTuple =
      ZipRefTuple<typename std::iterator_traits<Iterators>::reference...>;

public:
  ZipIterator(const Iterators&... iterators)
      : iter_tuple_(std::make_tuple(iterators...)) {}

  bool equal(const ZipIterator& that) const noexcept {
    return iter_tuple_ == that.iter_tuple_;
  }

  RefTuple dereference() const noexcept {
    return DerefIterTuple(
        const_cast<IterTuple&>(iter_tuple_),
        std::index_sequence_for<Iterators...>());
    // return DerefIterTuple(iter_tuple_, std::index_sequence_for<Iterators...>());
  }

  RefTuple dereference() noexcept {
    return DerefIterTuple(iter_tuple_, std::index_sequence_for<Iterators...>());
  }

  void increment() noexcept {
    ApplyToEach(iter_tuple_, [](auto& iter) { ++iter; });
  }

  void decrement() noexcept {
    ApplyToEach(iter_tuple_, [](auto& iter) { --iter; });
  }

  void advance(std::ptrdiff_t n) noexcept {
    ApplyToEach(iter_tuple_, [n](auto& iter) { iter += n; });
  }

  std::ptrdiff_t distance_to(const ZipIterator& that) const noexcept {
    return std::distance(
        std::get<0>(iter_tuple_), std::get<0>(that.iter_tuple_));
  }

private:
  template <typename Tuple, size_t... Indices>
  static auto DerefIterTuple(
      Tuple& iterTuple, std::index_sequence<Indices...>) {
    return RefTuple{std::tie(*std::get<Indices>(iterTuple)...)};
  }

  template <typename F, size_t... Indices>
  static void ApplyToEachImpl(
      IterTuple& tup, F func, const std::index_sequence<Indices...>&) {
    (func(std::get<Indices>(tup)), ...);
  }

  template <typename F>
  static void ApplyToEach(IterTuple& tup, F func) noexcept {
    ApplyToEachImpl(tup, func, std::index_sequence_for<Iterators...>());
  }

  IterTuple iter_tuple_;
};

template <typename... Iterators>
auto
make_zip_iterator(const Iterators&... iterators) noexcept {
  return ZipIterator<Iterators...>(iterators...);
}

template <typename I>
class KATANA_EXPORT DisjointRangesIterator: public boost::iterator_facade<
  DisjointRangesIterator<I>,
  typename std::iterator_traits<I>::value_type,
  typename std::iterator_traits<I>::iterator_category,
  typename std::iterator_traits<I>::reference > {

  using Base = boost::iterator_facade<
    DisjointRangesIterator<I>,
    typename std::iterator_traits<I>::value_type,
    typename std::iterator_traits<I>::iterator_category,
    typename std::iterator_traits<I>::reference 
  >;

  friend class boost::iterator_core_access;


public:

  using IteratorPair = std::pair<I, I>;

  enum class RangeKind: int {
    kFirst = 0,
    kSecond
  };

  DisjointRangesIterator(const IteratorPair& range_one, const IteratorPair& range_two,
      const I& pos, const RangeKind& pos_range) noexcept:
    range_one_(range_one),
    range_two_(range_two),
    iter_(pos),
    curr_range_(pos_range)
  {}

  bool equal(const DisjointRangesIterator& that) const noexcept {
    return iter_ == that.iter_;
  }

  const typename Base::reference dereference() const noexcept {
    return *iter_;
  }
  
  typename Base::reference dereference() noexcept {
    return *iter_;
  }

  void increment() noexcept {
    ++iter_;
    if (iter_ == range_one_.second) {
      // Reached end of first range hop to next range;
      iter_ = range_two_.first;
      curr_range_ = RangeKind::kSecond;
    }
  }

  void decrement() noexcept {
    if (iter_ == range_two_.first) {
      // Reached beginning of second range, hop to end of first
      iter_ = range_one_.second;
      curr_range_ = RangeKind::kFirst;
    }
    --iter_;
  }

  void advance(std::ptrdiff_t n) noexcept {
    if (n >= 0) {
      auto rem = n;

      if (curr_range_ == RangeKind::kFirst
          && rem >= std::distance(iter_, range_one_.second)) {

        rem -= std::distance(iter_, range_one_.second);
        iter_ = range_two_.first;
        curr_range_ = RangeKind::kSecond;
      }

      std::advance(iter_, rem);

    } else { // n is negative

      auto rem = -n;
      if (curr_range_ == RangeKind::kSecond
          && rem >= std::distance(range_two_.first, iter_)) {

        rem -= std::distance(range_two_.first, iter_);
        iter_ = range_one_.second;
        // move back one to the last valid position in range_one_;
        --iter_;
        // decrement rem accordingly
        --rem;

        curr_range_ = RangeKind::kFirst;
      }

      std::advance(iter_, -rem);
    }
  }

  std::ptrdiff_t distance_to(const DisjointRangesIterator& that) const noexcept {
    KATANA_LOG_DEBUG_ASSERT(SameRanges(that));

    if (this->curr_range_ == that.curr_range_) {
      return std::distance(this->iter_, that.iter_);
    }

    // else this and that are in different ranges

    if (this->curr_range_ == RangeKind::kFirst) {

      KATANA_LOG_DEBUG_ASSERT(that.curr_range_ == RangeKind::kSecond);
      return std::distance(this->iter_, this->range_one_.second) 
        + std::distance(this->range_two_.first, that.iter_);

    }
    // else
    KATANA_LOG_DEBUG_ASSERT(this->curr_range_ == RangeKind::kSecond);
    KATANA_LOG_DEBUG_ASSERT(that.curr_range_ == RangeKind::kFirst);

    return -std::distance(this->range_two_.first, this->iter_) 
      - std::distance(that.iter_, this->range_one_.second);

  }

private:
  bool SameRanges(const DisjointRangesIterator& that) const noexcept {
    return range_one_ == that.range_one_ && range_two_ == that.range_two_;
  }

  IteratorPair range_one_;
  IteratorPair range_two_;
  I iter_;
  RangeKind curr_range_;

};


template <typename I>
KATANA_EXPORT DisjointRangesIterator<I>
make_disjoint_ranges_begin(const std::pair<I, I>& range_one, const std::pair<I, I>& range_two) {
  return DisjointRangesIterator<I>(range_one, range_two, range_one.first, 
      DisjointRangesIterator<I>::RangeKind::kFirst);
}

template <typename I>
KATANA_EXPORT DisjointRangesIterator<I>
make_disjoint_ranges_end(const std::pair<I, I>& range_one, const std::pair<I, I>& range_two) {
  return DisjointRangesIterator<I>(range_one, range_two, range_two.second, 
      DisjointRangesIterator<I>::RangeKind::kSecond);
}

}  // end namespace katana

#endif
