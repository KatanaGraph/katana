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

#include "Bipart.h"

struct OnlineStat {
  uint32_t num_nodes;
  uint64_t total_distance;
  uint64_t min_distance;
  uint64_t max_distance;
  double distance_square;

  OnlineStat()
      : num_nodes(0),
        total_distance(0),
        min_distance(std::numeric_limits<uint64_t>::max()),
        max_distance(0),
        distance_square(0) {}

  void AddDistance(uint64_t distance) {
    ++num_nodes;
    total_distance += distance;
    distance_square += distance * distance;
    min_distance = std::min(distance, min_distance);
    max_distance = std::max(distance, max_distance);
  }

  double GetMean() { return total_distance / static_cast<double>(num_nodes); }

  double GetVariance() {
    double t = distance_square / num_nodes;
    double m = GetMean();
    return t - m * m;
  }

  uint32_t GetNodeCount() { return num_nodes; }
  uint64_t GetTotalDistance() { return total_distance; }
  uint64_t GetMinDistance() { return min_distance; }
  uint64_t GetMaxDistance() { return max_distance; }
};

uint32_t
GraphStat(GGraph& graph) {
  OnlineStat stat;
  for (GNode node : graph) {
    uint64_t dist = std::distance(graph.edge_begin(node), graph.edge_end(node));
    stat.AddDistance(dist);
  }
  galois::gPrint(
      "Nodes ", stat.GetNodeCount(), " Edges(total, var, min, max) ",
      stat.GetTotalDistance(), " ", stat.GetVariance(), " ",
      stat.GetMinDistance(), " ", stat.GetMaxDistance(), "\n");
  return stat.GetNodeCount();
}
