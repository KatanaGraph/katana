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

#ifndef KATANA_LIBGALOIS_KATANA_MEM_H_
#define KATANA_LIBGALOIS_KATANA_MEM_H_

#include "katana/Allocators.h"
#include "katana/config.h"

namespace katana {

/**
 * Preallocates memory on each thread. The allocation size is given in pages
 * per thread and total bytes which will be divided amongst the threads.
 */
KATANA_EXPORT
void Prealloc(size_t pagesPerThread, size_t bytes);

/**
 * Preallocates memory on each thread. The allocation size is given in total pages
 * which will be divided amongst the threads.
 */
KATANA_EXPORT
void Prealloc(size_t pages);

//! [PerIterAllocTy example]
//! Base allocator for per-iteration allocator
typedef katana::BumpWithMallocHeap<katana::FreeListHeap<katana::SystemHeap>>
    IterAllocBaseTy;

//! Per-iteration allocator that conforms to STL allocator interface
typedef katana::ExternalHeapAllocator<char, IterAllocBaseTy> PerIterAllocTy;
//! [PerIterAllocTy example]

//! Scalable variable-sized allocator for T that allocates blocks of sizes in
//! powers of 2 Useful for small and medium sized allocations, e.g. small or
//! medium vectors, strings, deques
template <typename T>
using Pow2VarSizeAlloc = Pow2BlockAllocator<T>;

}  // namespace katana
#endif
