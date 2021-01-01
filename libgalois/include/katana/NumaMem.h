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

#ifndef KATANA_LIBGALOIS_KATANA_NUMAMEM_H_
#define KATANA_LIBGALOIS_KATANA_NUMAMEM_H_

#include <cstddef>
#include <memory>
#include <vector>

#include "katana/config.h"

namespace katana {

namespace internal {
struct KATANA_EXPORT largeFreer {
  size_t bytes;
  void operator()(void* ptr) const;
};
}  // namespace internal

typedef std::unique_ptr<void, internal::largeFreer> LAptr;

KATANA_EXPORT LAptr largeMallocLocal(size_t bytes);  // fault in locally
KATANA_EXPORT LAptr
largeMallocFloating(size_t bytes);  // leave numa mapping undefined
// fault in interleaved mapping
KATANA_EXPORT LAptr largeMallocInterleaved(size_t bytes, unsigned numThreads);
// fault in block interleaved mapping
KATANA_EXPORT LAptr largeMallocBlocked(size_t bytes, unsigned numThreads);

// fault in specified regions for each thread (threadRanges)
template <typename RangeArrayTy>
LAptr largeMallocSpecified(
    size_t bytes, uint32_t numThreads, RangeArrayTy& threadRanges,
    size_t elementSize);

}  // namespace katana

#endif
