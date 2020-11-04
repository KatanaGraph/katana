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

#ifndef GALOIS_LIBGALOIS_GALOIS_SUBSTRATE_PAGEALLOC_H_
#define GALOIS_LIBGALOIS_GALOIS_SUBSTRATE_PAGEALLOC_H_

#include <cstddef>

#include "galois/config.h"

namespace galois::substrate {

// size of pages
GALOIS_EXPORT size_t allocSize();

// allocate contiguous pages, optionally faulting them in
GALOIS_EXPORT void* allocPages(unsigned num, bool preFault);

// free page range
GALOIS_EXPORT void freePages(void* ptr, unsigned num);

}  // namespace galois::substrate

#endif
