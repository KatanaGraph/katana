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

#ifndef KATANA_LIBGALOIS_KATANA_STATISTICS_H_
#define KATANA_LIBGALOIS_KATANA_STATISTICS_H_

#include <limits>
#include <string>
#include <type_traits>

#include "katana/config.h"
#include "katana/gIO.h"
#include "katana/gstl.h"

namespace katana {

template <typename T>
class RunningMin {
  T m_min;

public:
  RunningMin() : m_min(std::numeric_limits<T>::max()) {}

  void add(const T& val) { m_min = std::min(m_min, val); }

  const T& min() const { return m_min; }
};

template <typename T>
class RunningMax {
  T m_max;

public:
  RunningMax() : m_max(std::numeric_limits<T>::min()) {}

  void add(const T& val) { m_max = std::max(m_max, val); }

  const T& max() const { return m_max; }
};

template <typename T>
class RunningSum {
  T m_sum;
  size_t m_count;

public:
  RunningSum() : m_sum(), m_count(0) {}

  void add(const T& val) {
    m_sum += val;
    ++m_count;
  }

  const T& sum() const { return m_sum; }

  const size_t& count() const { return m_count; }

  T avg() const { return m_sum / m_count; }
};

template <typename T>
class RunningVec {
  using Vec = gstl::Vector<T>;

  Vec m_vec;

public:
  void add(const T& val) { m_vec.push_back(val); }

  const Vec& values() const { return m_vec; }
};

template <typename T>
class NamedStat {
  using Str = katana::gstl::Str;

  Str m_name;

public:
  void setName(const Str& name) { m_name = name; }

  void setName(Str&& name) { m_name = std::move(name); }

  const Str& name() const { return m_name; }

  void add(const T&) const {}
};

template <typename T, typename... Bases>
class AggregStat : public Bases... {
public:
  using with_min = AggregStat<T, RunningMin<T>, Bases...>;

  using with_max = AggregStat<T, RunningMax<T>, Bases...>;

  using with_sum = AggregStat<T, RunningSum<T>, Bases...>;

  using with_mem = AggregStat<T, RunningVec<T>, Bases...>;

  using with_name = AggregStat<T, NamedStat<T>, Bases...>;

  void add(const T& val) { (..., Bases::add(val)); }
};

struct StatTotal {
  enum Type { SINGLE = 0, TMIN, TMAX, TSUM, TAVG };

  static constexpr const char* kTotalNames[] = {
      "SINGLE", "TMIN", "TMAX", "TSUM", "TAVG"};
  static const char* str(const Type& t) { return kTotalNames[t]; }
};

namespace internal {

template <typename Stat_tp>
struct BasicStatMap {
  using Stat = Stat_tp;
  using Str = katana::gstl::Str;
  using StrSet = katana::gstl::Set<Str>;
  using StatMap = katana::gstl::Map<std::tuple<const Str*, const Str*>, Stat>;
  using const_iterator = typename StatMap::const_iterator;

protected:
  StrSet symbols;
  StatMap statMap;

  const Str* getOrInsertSymbol(const Str& s) {
    auto p = symbols.insert(s);
    return &*(p.first);
  }

  const Str* getSymbol(const Str& s) const {
    auto i = symbols.find(s);

    if (i == symbols.cend()) {
      return nullptr;
    } else {
      return &(*i);
    }
  }

public:
  template <typename... Args>
  Stat& getOrInsertStat(
      const Str& region, const Str& category, Args&&... args) {
    const Str* ln = getOrInsertSymbol(region);
    const Str* cat = getOrInsertSymbol(category);

    auto tpl = std::make_tuple(ln, cat);

    auto p = statMap.emplace(tpl, Stat(std::forward<Args>(args)...));

    return p.first->second;
  }

  const_iterator findStat(const Str& region, const Str& category) const {
    const Str* ln = getSymbol(region);
    const Str* cat = getSymbol(category);
    auto tpl = std::make_tuple(ln, cat);

    auto i = statMap.find(tpl);

    return i;
  }

  const Stat& getStat(const Str& region, const Str& category) const {
    auto i = findStat(region, category);
    assert(i != statMap.end());
    return i->second;
  }

  template <typename T, typename... Args>
  void addToStat(
      const Str& region, const Str& category, const T& val,
      Args&&... statArgs) {
    Stat& s =
        getOrInsertStat(region, category, std::forward<Args>(statArgs)...);
    s.add(val);
  }

  const_iterator cbegin() const { return statMap.cbegin(); }
  const_iterator cend() const { return statMap.cend(); }

  const Str& region(const const_iterator& i) const {
    return *(std::get<0>(i->first));
  }

  const Str& category(const const_iterator& i) const {
    return *(std::get<1>(i->first));
  }

  const Stat& stat(const const_iterator& i) const { return i->second; }
};

template <typename T>
using VecStatMinMaxSum =
    typename AggregStat<T>::with_mem::with_min::with_max::with_sum;

template <typename T>
struct VecStat : public VecStatMinMaxSum<T> {
  StatTotal::Type m_totalTy;

  explicit VecStat(const StatTotal::Type& type) : m_totalTy(type) {}

  const StatTotal::Type& totalTy() const { return m_totalTy; }

  T total() const {
    switch (m_totalTy) {
    case StatTotal::SINGLE:
      assert(this->values().size() > 0);
      return this->values()[0];

    case StatTotal::TMIN:
      return this->min();

    case StatTotal::TMAX:
      return this->max();

    case StatTotal::TSUM:
      return this->sum();

    case StatTotal::TAVG:
      return this->avg();

    default:
      KATANA_DIE("unreachable");
    }
  }
};

template <>
struct VecStat<gstl::Str> : public AggregStat<gstl::Str>::with_mem {
  StatTotal::Type m_totalTy;

  explicit VecStat(const StatTotal::Type& type) : m_totalTy(type) {}

  const StatTotal::Type& totalTy() const { return m_totalTy; }

  const gstl::Str& total() const {
    switch (m_totalTy) {
    case StatTotal::SINGLE:
      assert(values().size() > 0);
      return values()[0];

    default:
      KATANA_DIE("unreachable");
    }
  }
};

template <typename T>
using VecStatManager = BasicStatMap<VecStat<T>>;

template <typename T>
struct ScalarStat {
  T m_val;
  StatTotal::Type m_totalTy;

  explicit ScalarStat(const StatTotal::Type& type) : m_val(), m_totalTy(type) {}

  void add(const T& v) { m_val += v; }

  operator const T&() const { return m_val; }

  const StatTotal::Type& totalTy() const { return m_totalTy; }
};

template <typename T>
using ScalarStatManager = BasicStatMap<ScalarStat<T>>;

}  // end namespace internal

class KATANA_EXPORT StatManager {
  class Impl;

  std::unique_ptr<Impl> impl_;

public:
  using Str = katana::gstl::Str;
  using int_const_iterator =
      typename internal::VecStatManager<int64_t>::const_iterator;
  using fp_const_iterator =
      typename internal::VecStatManager<double>::const_iterator;
  using param_const_iterator =
      typename internal::VecStatManager<Str>::const_iterator;

protected:
  static constexpr const char* const kSep = ", ";
  static constexpr const char* const kThreadSep = "; ";
  static constexpr const char* const kThreadNameSep = "ThreadValues";

  /// PrintStats prints statistics to a stream.
  ///
  /// This function is called by Print and Print, in turn, is typically called
  /// during the destruction of SharedMem. All statistics are added via this
  /// class; subclasses will need to extract the data they want with ReadInt,
  /// ReadParam and ReadFP and print their own results here.
  virtual void PrintStats(std::ostream& out);

  void MergeStats();

  bool IsPrintingThreadVals() const;

  int_const_iterator int_cbegin() const;
  int_const_iterator int_cend() const;
  fp_const_iterator fp_cbegin() const;
  fp_const_iterator fp_cend() const;
  param_const_iterator param_cbegin() const;
  param_const_iterator param_cend() const;

  void ReadInt(
      int_const_iterator i, Str& region, Str& category, int64_t& total,
      StatTotal::Type& type, gstl::Vector<int64_t>& vec) const;

  void ReadFP(
      fp_const_iterator i, Str& region, Str& category, double& total,
      StatTotal::Type& type, gstl::Vector<double>& vec) const;

  void ReadParam(
      param_const_iterator i, Str& region, Str& category, Str& total,
      StatTotal::Type& type, gstl::Vector<Str>& vec) const;

public:
  StatManager();

  StatManager(const StatManager&) = delete;
  StatManager& operator=(const StatManager&) = delete;
  StatManager(StatManager&&) = delete;
  StatManager& operator=(StatManager&&) = delete;

  virtual ~StatManager();

  void SetStatFile(const std::string& outfile);

  void AddInt(
      const std::string& region, const std::string& category, int64_t val,
      const StatTotal::Type& type);

  void AddFP(
      const std::string& region, const std::string& category, double val,
      const StatTotal::Type& type);

  void AddParam(
      const std::string& region, const std::string& category, const Str& val);

  void Print();
};

namespace internal {

KATANA_EXPORT void setSysStatManager(StatManager* sm);
KATANA_EXPORT StatManager* sysStatManager();

}  // end namespace internal

template <typename T>
void
ReportParam(
    const std::string& region, const std::string& category, const T& value) {
  internal::sysStatManager()->AddParam(region, category, gstl::makeStr(value));
}

template <typename T>
void
ReportStat(
    const std::string& region, const std::string& category, const T& value,
    const StatTotal::Type& type,
    std::enable_if_t<std::is_integral_v<T>>* = nullptr) {
  internal::sysStatManager()->AddInt(region, category, int64_t(value), type);
}

template <typename T>
void
ReportStat(
    const std::string& region, const std::string& category, const T& value,
    const StatTotal::Type& type,
    std::enable_if_t<std::is_floating_point_v<T>>* = nullptr) {
  internal::sysStatManager()->AddFP(region, category, double(value), type);
}

template <typename T>
void
ReportStatSingle(
    const std::string& region, const std::string& category, const T& value) {
  ReportStat(region, category, value, StatTotal::SINGLE);
}

template <typename T>
void
ReportStatMin(
    const std::string& region, const std::string& category, const T& value) {
  ReportStat(region, category, value, StatTotal::TMIN);
}

template <typename T>
void
ReportStatMax(
    const std::string& region, const std::string& category, const T& value) {
  ReportStat(region, category, value, StatTotal::TMAX);
}

template <typename T>
void
ReportStatSum(
    const std::string& region, const std::string& category, const T& value) {
  ReportStat(region, category, value, StatTotal::TSUM);
}

template <typename T>
void
ReportStatAvg(
    const std::string& region, const std::string& category, const T& value) {
  ReportStat(region, category, value, StatTotal::TAVG);
}

//! Reports maximum resident set size and page faults stats using
//! rusage
//! @param id Identifier to prefix stat with in statistics output
KATANA_EXPORT void reportRUsage(const std::string& id);

//! Reports Galois system memory stats for all threads
KATANA_EXPORT void reportPageAlloc(const char* category);

/// Prints statistics out to standard out or to the file indicated by
/// SetStatFile
KATANA_EXPORT void PrintStats();

KATANA_EXPORT void SetStatFile(const std::string& f);

}  // end namespace katana

#endif
