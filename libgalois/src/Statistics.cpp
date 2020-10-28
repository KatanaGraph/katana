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

#include "galois/Statistics.h"

#include <sys/resource.h>
#include <sys/time.h>

#include <fstream>
#include <iostream>

#include "galois/GetEnv.h"
#include "galois/Logging.h"
#include "galois/runtime/Executor_OnEach.h"
#include "galois/substrate/PerThreadStorage.h"

namespace {

bool
CheckPrintingThreadVals() {
  return galois::GetEnv("PRINT_PER_THREAD_STATS");
}

void
PrintHeader(std::ostream& out, const char* sep) {
  out << "STAT_TYPE" << sep << "REGION" << sep << "CATEGORY" << sep;
  out << "TOTAL_TYPE" << sep << "TOTAL";
  out << "\n";
}

template <typename T>
struct StatImpl {
  using MergedStats = galois::internal::VecStatManager<T>;
  using Stat = typename MergedStats::Stat;
  using const_iterator = typename MergedStats::const_iterator;

  static constexpr const char* StatKind() {
    return std::is_same<T, galois::gstl::Str>::value ? "PARAM" : "STAT";
  }

  galois::substrate::PerThreadStorage<galois::internal::ScalarStatManager<T>>
      perThreadManagers_;
  MergedStats result_;
  bool merged_{};

  void Add(
      const galois::gstl::Str& region, const galois::gstl::Str& category,
      const T& val, const galois::StatTotal::Type& type) {
    perThreadManagers_.getLocal()->addToStat(region, category, val, type);
  }

  void Merge() {
    if (merged_) {
      return;
    }

    for (unsigned t = 0; t < perThreadManagers_.size(); ++t) {
      const auto* manager = perThreadManagers_.getRemote(t);

      for (auto i = manager->cbegin(), end_i = manager->cend(); i != end_i;
           ++i) {
        result_.addToStat(
            manager->region(i), manager->category(i), T(manager->stat(i)),
            manager->stat(i).totalTy());
      }
    }

    merged_ = true;
  }

  void Read(
      const_iterator i, galois::gstl::Str& region, galois::gstl::Str& category,
      T& total, galois::StatTotal::Type& type,
      galois::gstl::Vector<T>& values) const {
    region = result_.region(i);
    category = result_.category(i);

    total = result_.stat(i).total();
    type = result_.stat(i).totalTy();

    values = result_.stat(i).values();
  }

  void Print(
      std::ostream& out, const char* sep, const char* thread_sep,
      const char* thread_name_sep) const {
    for (auto i = result_.cbegin(), end_i = result_.cend(); i != end_i; ++i) {
      out << StatKind() << sep << result_.region(i) << sep
          << result_.category(i) << sep;

      const auto& s = result_.stat(i);
      out << galois::StatTotal::str(s.totalTy()) << sep << s.total();
      out << "\n";

      if (CheckPrintingThreadVals()) {
        out << StatKind() << sep << result_.region(i) << sep
            << result_.category(i) << sep;
        out << thread_name_sep << sep;

        const char* tsep = "";
        for (const auto& v : s.values()) {
          out << tsep << v;
          tsep = thread_sep;
        }

        out << "\n";
      }
    }
  }
};

}  // end unnamed namespace

class galois::StatManager::Impl {
public:
  StatImpl<int64_t> int_stats_;
  StatImpl<double> fp_stats_;
  StatImpl<Str> str_stats_;
  std::string outfile_;
};

galois::StatManager::StatManager() { impl_ = std::make_unique<Impl>(); }

galois::StatManager::~StatManager() = default;

void
galois::StatManager::SetStatFile(const std::string& outfile) {
  impl_->outfile_ = outfile;
}

bool
galois::StatManager::IsPrintingThreadVals() const {
  return CheckPrintingThreadVals();
}

void
galois::StatManager::PrintStats(std::ostream& out) {
  MergeStats();

  if (int_cbegin() == int_cend() && fp_cbegin() == fp_cend() &&
      param_cbegin() == param_cend()) {
    return;
  }

  PrintHeader(out, kSep);
  impl_->int_stats_.Print(out, kSep, kThreadSep, kThreadNameSep);
  impl_->fp_stats_.Print(out, kSep, kThreadSep, kThreadNameSep);
  impl_->str_stats_.Print(out, kSep, kThreadSep, kThreadNameSep);
}

auto
galois::StatManager::int_cbegin() const -> int_const_iterator {
  return impl_->int_stats_.result_.cbegin();
}

auto
galois::StatManager::int_cend() const -> int_const_iterator {
  return impl_->int_stats_.result_.cend();
}

auto
galois::StatManager::fp_cbegin() const -> fp_const_iterator {
  return impl_->fp_stats_.result_.cbegin();
}

auto
galois::StatManager::fp_cend() const -> fp_const_iterator {
  return impl_->fp_stats_.result_.cend();
}

auto
galois::StatManager::param_cbegin() const -> param_const_iterator {
  return impl_->str_stats_.result_.cbegin();
}

auto
galois::StatManager::param_cend() const -> param_const_iterator {
  return impl_->str_stats_.result_.cend();
}

void
galois::StatManager::MergeStats() {
  impl_->int_stats_.Merge();
  impl_->fp_stats_.Merge();
  impl_->str_stats_.Merge();
}

void
galois::StatManager::ReadInt(
    int_const_iterator i, Str& region, Str& category, int64_t& total,
    StatTotal::Type& type, gstl::Vector<int64_t>& vec) const {
  impl_->int_stats_.Read(i, region, category, total, type, vec);
}

void
galois::StatManager::ReadFP(
    fp_const_iterator i, Str& region, Str& category, double& total,
    StatTotal::Type& type, gstl::Vector<double>& vec) const {
  impl_->fp_stats_.Read(i, region, category, total, type, vec);
}

void
galois::StatManager::ReadParam(
    param_const_iterator i, Str& region, Str& category, Str& total,
    StatTotal::Type& type, gstl::Vector<Str>& vec) const {
  impl_->str_stats_.Read(i, region, category, total, type, vec);
}

void
galois::StatManager::AddInt(
    const std::string& region, const std::string& category, int64_t val,
    const StatTotal::Type& type) {
  impl_->int_stats_.Add(
      gstl::makeStr(region), gstl::makeStr(category), val, type);
}

void
galois::StatManager::AddFP(
    const std::string& region, const std::string& category, double val,
    const StatTotal::Type& type) {
  impl_->fp_stats_.Add(
      gstl::makeStr(region), gstl::makeStr(category), val, type);
}

void
galois::StatManager::AddParam(
    const std::string& region, const std::string& category, const Str& val) {
  impl_->str_stats_.Add(
      gstl::makeStr(region), gstl::makeStr(category), val, StatTotal::SINGLE);
}

void
galois::StatManager::Print() {
  if (impl_->outfile_.empty()) {
    return PrintStats(std::cout);
  }

  std::ofstream out(impl_->outfile_.c_str());
  if (!out) {
    GALOIS_LOG_ERROR("could not print stats to {} ", impl_->outfile_);
    return PrintStats(std::cerr);
  }

  PrintStats(out);
}

static galois::StatManager* stat_manager_singleton;

void
galois::internal::setSysStatManager(galois::StatManager* sm) {
  GALOIS_LOG_VASSERT(
      !(stat_manager_singleton && sm),
      "StatManager.cpp: Double Initialization of SM");
  stat_manager_singleton = sm;
}

galois::StatManager*
galois::internal::sysStatManager() {
  return stat_manager_singleton;
}

void
galois::setStatFile(const std::string& f) {
  internal::sysStatManager()->SetStatFile(f);
}

void
galois::reportPageAlloc(const char* category) {
  galois::runtime::on_each_gen(
      [category](unsigned int tid, unsigned int) {
        ReportStatSum(
            "PageAlloc", category, substrate::numPagePoolAllocForThread(tid));
      },
      std::make_tuple());
}

void
galois::reportRUsage(const std::string& id) {
  // get rusage at this point in time
  struct rusage usage_stats;
  int rusage_result = getrusage(RUSAGE_SELF, &usage_stats);
  if (rusage_result != 0) {
    GALOIS_LOG_FATAL("getrusage failed: {}", rusage_result);
  }

  // report stats using ID to identify them
  ReportStat(
      "rusage", "MaxResidentSetSize_" + id, usage_stats.ru_maxrss,
      StatTotal::SINGLE);
  ReportStat(
      "rusage", "SoftPageFaults_" + id, usage_stats.ru_minflt,
      StatTotal::SINGLE);
  ReportStat(
      "rusage", "HardPageFaults_" + id, usage_stats.ru_majflt,
      StatTotal::SINGLE);
}
