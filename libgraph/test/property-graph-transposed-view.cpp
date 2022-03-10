#include "katana/SharedMemSys.h"
#include "katana/TopologyGeneration.h"

using namespace katana;
using Edge = PropertyGraph::Edge;
using Node = PropertyGraph::Node;
using TransposedGraphView = PropertyGraphViews::Transposed;

Result<void>
TestTransposedView() {
  // We build a simple tree-like graph and its transpose.
  // Then we compare the transposed view of the first graph with the second.
  AsymmetricGraphTopologyBuilder builder, builder_tr;

  builder.AddNodes(7);
  builder_tr.AddNodes(7);

  std::vector<std::array<int, 2>> edges = {{0, 1}, {0, 2}, {1, 3},
                                           {1, 4}, {2, 5}, {2, 6}};
  for (const auto& [n1, n2] : edges) {
    builder.AddEdge(n1, n2);
    builder_tr.AddEdge(n2, n1);
  }

  auto pg = std::shared_ptr<katana::PropertyGraph>(
      KATANA_CHECKED(PropertyGraph::Make(builder.ConvertToCSR())));
  TransposedGraphView pg_tr_view = pg->BuildView<TransposedGraphView>();

  auto pg_tr = KATANA_CHECKED(PropertyGraph::Make(builder_tr.ConvertToCSR()));

  for (Edge e : pg_tr_view.OutEdges()) {
    KATANA_LOG_VASSERT(
        pg_tr->topology().GetEdgeSrc(e) == pg_tr_view.GetEdgeSrc(e),
        "Edge sources do not match");
    KATANA_LOG_VASSERT(
        pg_tr->topology().OutEdgeDst(e) == pg_tr_view.OutEdgeDst(e),
        "Edge destinations do not match");
  }

  return katana::ResultSuccess();
}

int
main() {
  SharedMemSys sys;

  auto res = TestTransposedView();
  KATANA_LOG_ASSERT(res);

  return 0;
}
