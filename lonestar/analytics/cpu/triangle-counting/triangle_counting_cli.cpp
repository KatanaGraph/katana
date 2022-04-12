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

#include <iostream>

#include "Lonestar/BoilerPlate.h"
#include "katana/analytics/triangle_count/triangle_count.h"

using namespace katana::analytics;

const char* name = "Triangles";
const char* desc = "Counts the triangles in a graph";

namespace cll = llvm::cl;

static cll::opt<std::string> inputFile(
    cll::Positional, cll::desc("<input file>"), cll::Required);
static cll::opt<TriangleCountPlan::Algorithm> algo(
    "algo", cll::desc("Choose an algorithm:"),
    cll::values(
        clEnumValN(
            TriangleCountPlan::kNodeIteration, "nodeiterator", "Node Iterator"),
        clEnumValN(
            TriangleCountPlan::kEdgeIteration, "edgeiterator", "Edge Iterator"),
        clEnumValN(
            TriangleCountPlan::kOrderedCount, "orderedCount",
            "Ordered Simple Count (default)")),
    cll::init(TriangleCountPlan::kOrderedCount));

static cll::opt<bool> relabel(
    "relabel",
    cll::desc("Relabel nodes of the graph (default value of false => "
              "choose automatically)"),
    cll::init(false));
int
main(int argc, char** argv) {
  std::unique_ptr<katana::SharedMemSys> G =
      LonestarStart(argc, argv, name, desc, nullptr, &inputFile);

  katana::StatTimer totalTime("TimerTotal");
  totalTime.start();

  if (!symmetricGraph) {
    KATANA_DIE(
        "This application requires a symmetric graph input;"
        " please use the -symmetricGraph flag "
        " to indicate the input is a symmetric graph.");
  }

  std::cout << "Reading from file: " << inputFile << "\n";
  auto res = katana::URI::Make(inputFile);
  if (!res) {
    KATANA_LOG_FATAL("URI from string {} failed: {}", inputFile, res.error());
  }
  auto inputURI = res.value();
  std::unique_ptr<katana::PropertyGraph> pg =
      MakeFileGraph(inputURI, edge_property_name);

  std::cout << "Read " << pg->topology().NumNodes() << " nodes, "
            << pg->topology().NumEdges() << " edges\n";

  std::unique_ptr<katana::PropertyGraph> pg_projected_view =
      ProjectPropertyGraphForArguments(pg);

  std::cout << "Projected graph has: "
            << pg_projected_view->topology().NumNodes() << " nodes, "
            << pg_projected_view->topology().NumEdges() << " edges\n";

  TriangleCountPlan plan;

  TriangleCountPlan::Relabeling relabeling_flag =
      relabel ? TriangleCountPlan::kRelabel : TriangleCountPlan::kAutoRelabel;

  switch (algo) {
  case TriangleCountPlan::kNodeIteration:
    plan = TriangleCountPlan::NodeIteration(relabeling_flag);
    break;

  case TriangleCountPlan::kEdgeIteration:
    plan = TriangleCountPlan::EdgeIteration(relabeling_flag);
    break;

  case TriangleCountPlan::kOrderedCount:
    plan = TriangleCountPlan::OrderedCount(relabeling_flag);
    break;

  default:
    std::cerr << "Unknown algo: " << algo << "\n";
  }

  auto num_triangles_result = TriangleCount(pg_projected_view.get(), plan);
  if (!num_triangles_result) {
    KATANA_LOG_FATAL(
        "failed to run algorithm: {}", num_triangles_result.error());
  }
  auto num_triangles = num_triangles_result.value();

  std::cout << "NumTriangles: " << num_triangles << "\n";

  totalTime.stop();

  return 0;
}
