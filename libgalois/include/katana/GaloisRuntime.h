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

#ifndef KATANA_LIBGALOIS_KATANA_GALOISRUNTIME_H_
#define KATANA_LIBGALOIS_KATANA_GALOISRUNTIME_H_

#include <memory>

#include "katana/config.h"

namespace katana {

/// A GaloisRuntime represents global initialization required for the shared
/// memory subsystem, i.e., thread pools and barriers. As a side-effect of
/// construction, this class sets global internal state.
///
/// Data structures that require per-thread allocation typically ask for the
/// thread pool. If their construction is not guaranteed to happen after the
/// construction of a GaloisRuntime, initialization races can occur.
class KATANA_EXPORT GaloisRuntime {
  struct Impl;
  std::unique_ptr<Impl> impl_;

public:
  GaloisRuntime();
  ~GaloisRuntime();

  GaloisRuntime(const GaloisRuntime&) = delete;
  GaloisRuntime& operator=(const GaloisRuntime&) = delete;

  GaloisRuntime(GaloisRuntime&&) = delete;
  GaloisRuntime& operator=(GaloisRuntime&&) = delete;
};

}  // namespace katana

#endif
