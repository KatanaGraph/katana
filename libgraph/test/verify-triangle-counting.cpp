#include "katana/SharedMemSys.h"
#include "katana/TopologyGeneration.h"
#include "katana/analytics/triangle_count/triangle_count.h"

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

  // Grid tests
  RunTriCount(katana::MakeGrid(2, 2, true), 4);
  RunTriCount(katana::MakeGrid(3, 4, true), 24);
  RunTriCount(katana::MakeGrid(5, 7, true), 96);
  RunTriCount(katana::MakeGrid(5, 7, false), 0);

  // Ferris wheel tests
  RunTriCount(katana::MakeFerrisWheel(5), 4);
  RunTriCount(katana::MakeFerrisWheel(6), 5);
  RunTriCount(katana::MakeFerrisWheel(9), 8);

  // Sawtooth tests
  RunTriCount(katana::MakeSawtooth(1), 1);
  RunTriCount(katana::MakeSawtooth(2), 2);
  RunTriCount(katana::MakeSawtooth(3), 3);

  // Clique tests
  RunTriCount(katana::MakeClique(3), 1);
  RunTriCount(katana::MakeClique(4), 4);
  RunTriCount(katana::MakeClique(5), 10);

  // Triangular array tests
  RunTriCount(katana::MakeTriangle(1), 1);
  RunTriCount(katana::MakeTriangle(3), 9);
  RunTriCount(katana::MakeTriangle(4), 16);

  return 0;
}
