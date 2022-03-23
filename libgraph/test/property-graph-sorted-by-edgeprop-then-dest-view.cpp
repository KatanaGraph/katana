#include "katana/Logging.h"
#include "katana/SharedMemSys.h"
#include "katana/TopologyGeneration.h"
#include "katana/TypedPropertyGraph.h"

using namespace katana;
using Edge = PropertyGraph::Edge;
using Node = PropertyGraph::Node;
using EdgesSortedByPropThenDestIDGraphView =
    PropertyGraphViews::EdgesSortedByProperty;

struct EdgeDataProp : public katana::PODProperty<uint32_t> {};
using EdgeData = std::tuple<EdgeDataProp>;

using OrigTypeGraphView = katana::TypedPropertyGraph<std::tuple<>, EdgeData>;
using SortedTypeGraphView = katana::TypedPropertyGraphView<
    EdgesSortedByPropThenDestIDGraphView, std::tuple<>, EdgeData>;

template <typename T>
Result<void>
TestEdgesSortedByTypeThenDestIDGraphView() {
  // We build a simple tree-like graph and its sorted graph.
  // Then we compare the transposed view of the first graph with the second.
  AsymmetricGraphTopologyBuilder builder;

  builder.AddNodes(7);

  std::vector<std::array<T, 2>> unsorted_edges = {{0, 2}, {0, 1}, {1, 4},
                                                  {1, 3}, {2, 6}, {2, 5}};

  for (const auto& [n1, n2] : unsorted_edges) {
    builder.AddEdge(n1, n2);
  }

  auto pg = KATANA_CHECKED(PropertyGraph::Make(builder.ConvertToCSR()));
  katana::TxnContext txn_ctx;
  KATANA_CHECKED(
      pg->ConstructEdgeProperties<EdgeData>(&txn_ctx, {"edge_weight"}));
  auto orig_graph =
      KATANA_CHECKED(OrigTypeGraphView::Make(pg.get(), {}, {"edge_weight"}));
  orig_graph.template GetEdgeData<EdgeDataProp>(0) = 1;
  orig_graph.template GetEdgeData<EdgeDataProp>(1) = 1;
  orig_graph.template GetEdgeData<EdgeDataProp>(2) = 2;
  orig_graph.template GetEdgeData<EdgeDataProp>(3) = 3;
  orig_graph.template GetEdgeData<EdgeDataProp>(4) = 5;
  orig_graph.template GetEdgeData<EdgeDataProp>(5) = 4;

  // Before sorting:
  // (0, 2, 1), (0, 1, 1), (1, 4, 2), (1, 3, 3), (2, 6, 5), (2, 5, 4)

  // Results should be:
  // (0, 1, 1), (0, 2, 1), (1, 4, 2), (1, 3, 3), (2, 5, 4), (2, 6, 5)

  EdgesSortedByPropThenDestIDGraphView pg_view =
      pg->BuildView<EdgesSortedByPropThenDestIDGraphView>("edge_weight");
  auto sorted_graph =
      KATANA_CHECKED(SortedTypeGraphView::Make(pg_view, {}, {"edge_weight"}));

  KATANA_LOG_ASSERT(sorted_graph.GetEdgeSrc(0) == 0);
  KATANA_LOG_ASSERT(sorted_graph.GetEdgeSrc(1) == 0);
  KATANA_LOG_ASSERT(sorted_graph.GetEdgeSrc(2) == 1);
  KATANA_LOG_ASSERT(sorted_graph.GetEdgeSrc(3) == 1);
  KATANA_LOG_ASSERT(sorted_graph.GetEdgeSrc(4) == 2);
  KATANA_LOG_ASSERT(sorted_graph.GetEdgeSrc(5) == 2);
  KATANA_LOG_ASSERT(sorted_graph.OutEdgeDst(0) == 1);
  KATANA_LOG_ASSERT(sorted_graph.OutEdgeDst(1) == 2);
  KATANA_LOG_ASSERT(sorted_graph.OutEdgeDst(2) == 4);
  KATANA_LOG_ASSERT(sorted_graph.OutEdgeDst(3) == 3);
  KATANA_LOG_ASSERT(sorted_graph.OutEdgeDst(4) == 5);
  KATANA_LOG_ASSERT(sorted_graph.OutEdgeDst(5) == 6);
  KATANA_LOG_ASSERT(sorted_graph.template GetEdgeData<EdgeDataProp>(0) == 1);
  KATANA_LOG_ASSERT(sorted_graph.template GetEdgeData<EdgeDataProp>(1) == 1);
  KATANA_LOG_ASSERT(sorted_graph.template GetEdgeData<EdgeDataProp>(2) == 2);
  KATANA_LOG_ASSERT(sorted_graph.template GetEdgeData<EdgeDataProp>(3) == 3);
  KATANA_LOG_ASSERT(sorted_graph.template GetEdgeData<EdgeDataProp>(4) == 4);
  KATANA_LOG_ASSERT(sorted_graph.template GetEdgeData<EdgeDataProp>(5) == 5);

  return katana::ResultSuccess();
}

int
main() {
  SharedMemSys sys;

  auto uint32_res = TestEdgesSortedByTypeThenDestIDGraphView<uint32_t>();
  KATANA_LOG_ASSERT(uint32_res);
  auto int32_res = TestEdgesSortedByTypeThenDestIDGraphView<int32_t>();
  KATANA_LOG_ASSERT(int32_res);
  auto uint64_res = TestEdgesSortedByTypeThenDestIDGraphView<uint64_t>();
  KATANA_LOG_ASSERT(uint64_res);
  auto int64_res = TestEdgesSortedByTypeThenDestIDGraphView<int64_t>();
  KATANA_LOG_ASSERT(int64_res);
  auto float_res = TestEdgesSortedByTypeThenDestIDGraphView<float>();
  KATANA_LOG_ASSERT(float_res);
  auto double_res = TestEdgesSortedByTypeThenDestIDGraphView<double>();
  KATANA_LOG_ASSERT(double_res);

  return 0;
}
