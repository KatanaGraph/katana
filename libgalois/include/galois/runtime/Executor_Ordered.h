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

#ifndef GALOIS_LIBGALOIS_GALOIS_RUNTIME_EXECUTORORDERED_H_
#define GALOIS_LIBGALOIS_GALOIS_RUNTIME_EXECUTORORDERED_H_

#include "galois/config.h"

namespace galois {
namespace runtime {

// TODO(ddn): Pull in and integrate in executors from exp

template <typename Iter, typename Cmp, typename NhFunc, typename OpFunc>
void for_each_ordered_impl([[maybe_unused]] Iter beg, [[maybe_unused]] Iter end,
                           [[maybe_unused]] const Cmp& cmp,
                           [[maybe_unused]] const NhFunc& nhFunc,
                           [[maybe_unused]] const OpFunc& opFunc,
                           [[maybe_unused]] const char* loopname) {
  GALOIS_DIE("not yet implemented");
}

template <typename Iter, typename Cmp, typename NhFunc, typename OpFunc,
          typename StableTest>
void for_each_ordered_impl([[maybe_unused]] Iter beg, [[maybe_unused]] Iter end,
                           [[maybe_unused]] const Cmp& cmp,
                           [[maybe_unused]] const NhFunc& nhFunc,
                           [[maybe_unused]] const OpFunc& opFunc,
                           [[maybe_unused]] const StableTest& stabilityTest,
                           [[maybe_unused]] const char* loopname) {
  GALOIS_DIE("not yet implemented");
}

} // end namespace runtime
} // end namespace galois

#endif
