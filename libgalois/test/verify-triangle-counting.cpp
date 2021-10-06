#include "katana/PropertyGraph.h"
#include "katana/SharedMemSys.h"
#include "katana/analytics/triangle_count/triangle_count.h"

std::unique_ptr<katana::PropertyGraph>
MakeGridWithDiagonals() noexcept {
  katana::SymmetricGraphTopologyBuilder builder;
  builder.AddNodes(4);

  // Build a square
  builder.AddEdge(0, 1);
  builder.AddEdge(1, 2);
  builder.AddEdge(2, 3);
  builder.AddEdge(3, 0);

  // Add two diagonals, each creating 2 triangles (total 4)
  builder.AddEdge(0, 2);
  builder.AddEdge(1, 3);

  katana::GraphTopology topo = builder.ConvertToCSR();

  auto res = katana::PropertyGraph::Make(std::move(topo));
  KATANA_LOG_ASSERT(res);
  return std::move(res.value());
}

int
main() {
  katana::SharedMemSys S;

  std::unique_ptr<katana::PropertyGraph> pg = MakeGridWithDiagonals();

  katana::analytics::TriangleCountPlan plan =
      katana::analytics::TriangleCountPlan::NodeIteration(
          katana::analytics::TriangleCountPlan::kRelabel);

  auto num_tri = katana::analytics::TriangleCount(pg.get(), plan);
  KATANA_LOG_ASSERT(num_tri);

  KATANA_LOG_ASSERT(num_tri.value() == 4);

  return 0;
}
