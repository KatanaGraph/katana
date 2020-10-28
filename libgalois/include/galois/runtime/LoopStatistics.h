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

#ifndef GALOIS_LIBGALOIS_GALOIS_RUNTIME_LOOPSTATISTICS_H_
#define GALOIS_LIBGALOIS_GALOIS_RUNTIME_LOOPSTATISTICS_H_

#include "galois/Statistics.h"
#include "galois/config.h"

namespace galois {
namespace runtime {

// Usually instantiated per thread
template <bool Enabled>
class LoopStatistics {
protected:
  size_t m_iterations;
  size_t m_pushes;
  size_t m_conflicts;
  const char* loopname;

public:
  explicit LoopStatistics(const char* ln)
      : m_iterations(0), m_pushes(0), m_conflicts(0), loopname(ln) {}

  ~LoopStatistics() {
    ReportStatSum(loopname, "Iterations", m_iterations);
    ReportStatSum(loopname, "Commits", (m_iterations - m_conflicts));
    ReportStatSum(loopname, "Pushes", m_pushes);
    ReportStatSum(loopname, "Conflicts", m_conflicts);
  }

  size_t iterations() const { return m_iterations; }
  size_t pushes() const { return m_pushes; }
  size_t conflicts() const { return m_conflicts; }

  inline void inc_pushes(size_t v) { m_pushes += v; }

  inline void inc_iterations() { ++m_iterations; }

  inline void inc_conflicts() { ++m_conflicts; }
};

template <>
class LoopStatistics<false> {
public:
  explicit LoopStatistics(const char*) {}

  size_t iterations() const { return 0; }
  size_t pushes() const { return 0; }
  size_t conflicts() const { return 0; }

  inline void inc_iterations() const {}
  inline void inc_pushes(size_t) const {}
  inline void inc_conflicts() const {}
};

}  // namespace runtime
}  // namespace galois
#endif
