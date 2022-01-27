#include <vector>

#include "katana/SharedMemSys.h"
#include "katana/TopologyGeneration.h"
#include "katana/TypedPropertyGraph.h"

struct NodeLabel : public katana::PODProperty<uint32_t> {};
struct DegreeSum : public katana::PODProperty<uint32_t> {};

void
TestDegreeSum(std::unique_ptr<katana::PropertyGraph>&& pg) noexcept {
  katana::TxnContext txn_ctx;
  katana::Result<void> res = katana::AddNodeProperties(
      pg.get(), &txn_ctx,
      katana::PropertyGenerator(
          "label", []([[maybe_unused]] auto node_id) { return uint32_t{1}; }),
      katana::PropertyGenerator("deg_sum", []([[maybe_unused]] auto node_id) {
        return uint32_t{0};
      }));

  KATANA_LOG_VASSERT(res, "Failed to add node Properties");

  using NodeProps = std::tuple<NodeLabel, DegreeSum>;
  using EdgeProps = std::tuple<>;

  using UndirectedView = katana::TypedPropertyGraphView<
      katana::PropertyGraphViews::Undirected, NodeProps, EdgeProps>;
  using Node = UndirectedView::Node;
  using Edge = UndirectedView::Edge;

  auto view_res = UndirectedView::Make(std::move(pg), {"label", "deg_sum"}, {});
  KATANA_LOG_VASSERT(
      view_res, "Failed to create Undirected View. Error msg: {}",
      view_res.error());

  auto graph = view_res.value();

  for (Node src : graph.Nodes()) {
    uint32_t deg_sum = 0u;
    for (Edge e : graph.UndirectedEdges(src)) {
      Node dst = graph.UndirectedEdgeNeighbor(e);
      deg_sum += graph.GetData<NodeLabel>(dst);
    }

    graph.GetData<DegreeSum>(src) = deg_sum;
    KATANA_LOG_VASSERT(
        deg_sum == graph.UndirectedDegree(src), "Sum should equal degree");
  }

  uint32_t tot_deg_sum = 0u;

  for (Node n : graph.Nodes()) {
    tot_deg_sum += graph.GetData<DegreeSum>(n);
  }

  KATANA_LOG_VASSERT(
      tot_deg_sum == 2 * graph.NumEdges(), "Total Degree Sum Mismatched");
}

int
main() {
  katana::SharedMemSys S;

  TestDegreeSum(katana::MakeGrid(3, 4, true));
  TestDegreeSum(katana::MakeGrid(3, 4, false));

  return 0;
}
