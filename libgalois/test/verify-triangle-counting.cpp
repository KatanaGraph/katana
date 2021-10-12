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

void
RunTriCount(
    std::unique_ptr<katana::PropertyGraph>&& pg,
    const size_t num_expected_triangles) noexcept {
  using Plan = katana::analytics::TriangleCountPlan;
  std::vector<Plan> plans{
      Plan::NodeIteration(Plan::kRelabel), Plan::EdgeIteration(Plan::kRelabel),
      Plan::OrderedCount(Plan::kRelabel)};

  for (const auto& p : plans) {
    katana::Result<size_t> num_tri =
        katana::analytics::TriangleCount(pg.get(), p);
    KATANA_LOG_VASSERT(num_tri, "TriangleCount failed and returned error");
    KATANA_LOG_VASSERT(
        num_tri.value() == num_expected_triangles,
        "Wrong number of triangles. Found: {}, Expected: {}", num_tri.value(),
        num_expected_triangles);
  }
}

int
main() {
  katana::SharedMemSys S;

  RunTriCount(MakeGridWithDiagonals(), 4);

  return 0;
}
