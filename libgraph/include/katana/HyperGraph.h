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

#ifndef KATANA_LIBGRAPH_KATANA_HYPERGRAPH_H_
#define KATANA_LIBGRAPH_KATANA_HYPERGRAPH_H_

#include "katana/DynamicBitset.h"
#include "katana/LC_CSR_Graph.h"

namespace katana {
template <typename NodeTy, bool HasNoLockable = true, bool UseNumaAlloc = true>
class HyperGraph
    : public katana::LC_CSR_Graph<NodeTy, void, HasNoLockable, UseNumaAlloc> {
public:
  uint32_t GetHedges() const { return hedges_; }
  void SetHedges(uint32_t hedges) { hedges_ = hedges; }

  uint32_t GetHnodes() const { return hnodes_; }
  void SetHnodes(uint32_t hnodes) { hnodes_ = hnodes; }

private:
  uint32_t hedges_;
  uint32_t hnodes_;
};
}  // namespace katana

#endif
