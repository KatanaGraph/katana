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

#ifndef KATANA_LONESTAR_LIBLONESTAR_LONESTAR_UTILS_H_
#define KATANA_LONESTAR_LIBLONESTAR_LONESTAR_UTILS_H_

#include <vector>

#include <boost/filesystem.hpp>

#include "katana/PropertyGraph.h"
#include "katana/RDG.h"
#include "katana/analytics/Utils.h"

inline std::unique_ptr<katana::PropertyGraph>
MakeFileGraph(
    const katana::URI& rdg_name, const std::string& edge_property_name) {
  std::vector<std::string> edge_properties;
  std::vector<std::string> node_properties;
  if (!edge_property_name.empty()) {
    edge_properties.emplace_back(edge_property_name);
  }

  katana::TxnContext txn_ctx;
  katana::RDGLoadOptions opts;
  opts.node_properties = node_properties;
  opts.edge_properties = edge_properties;
  auto pfg_result = katana::PropertyGraph::Make(rdg_name, &txn_ctx, opts);
  if (!pfg_result) {
    KATANA_LOG_FATAL("cannot make graph: {}", pfg_result.error());
  }
  return std::move(pfg_result.value());
}

template <typename T>
void
writeOutput(
    const std::string& output_dir, T* values, size_t length,
    std::string output_filename = "output") {
  namespace fs = boost::filesystem;
  fs::path filename{output_dir};
  filename = filename.append(output_filename);

  std::ofstream outputFile(filename.string().c_str());

  if (!outputFile) {
    KATANA_LOG_FATAL("could not open file: {}", filename);
  }

  for (size_t i = 0; i < length; i++) {
    outputFile << i << " " << std::to_string(*(values++)) << "\n";
  }

  if (!outputFile) {
    KATANA_LOG_FATAL("failed to write file: {}", filename);
  }
}

#endif
