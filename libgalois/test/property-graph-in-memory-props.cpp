#include <iostream>

#include "katana/SharedMemSys.h"
#include "katana/TopologyGeneration.h"

using Edge = katana::PropertyGraph::Edge;
using Node = katana::PropertyGraph::Node;
using katana::AddEdgeProperties;
using katana::AddNodeProperties;
using katana::EdgePropertySetter;
using katana::NodePropertySetter;

void
TestNodeProps(std::unique_ptr<katana::PropertyGraph>&& pg) {
  katana::Result<void> result = AddNodeProperties(
      pg.get(),
      NodePropertySetter(
          "age", [](Node id) { return static_cast<int32_t>(id * 2); }),
      NodePropertySetter(
          "name", [](Node id) { return fmt::format("Node {}", id); }));

  KATANA_LOG_VASSERT(result, "AddNodeProperties returned an error.");

  KATANA_LOG_VASSERT(
      pg->HasNodeProperty("age"), "PropertyGraph must have the age property");

  KATANA_LOG_VASSERT(
      pg->HasNodeProperty("name"), "PropertyGraph must have the name property");

  auto ages = pg->GetNodeProperty("age").value();
  auto ages_array = std::static_pointer_cast<arrow::Int32Array>(ages->chunk(0));

  auto names = pg->GetNodeProperty("name").value();
  auto names_array =
      std::static_pointer_cast<arrow::StringArray>(names->chunk(0));

  size_t i = 0;
  for (auto node_it = pg->begin(); node_it != pg->end(); ++node_it, ++i) {
    int32_t expected_age = static_cast<int32_t>(*node_it) * 2;
    std::string expected_name = fmt::format("Node {}", *node_it);

    KATANA_LOG_VASSERT(
        ages_array->Value(i) == expected_age, "Incorrect node age value");

    KATANA_LOG_VASSERT(
        names_array->GetString(i) == expected_name,
        "Incorrect node name value");
  }
}

void
TestEdgeProps(std::unique_ptr<katana::PropertyGraph>&& pg) {
  katana::Result<void> result = AddEdgeProperties(
      pg.get(),
      EdgePropertySetter(
          "average",
          [&pg](Edge id) {
            Node src = pg->topology().edge_source(id);
            Node dst = pg->topology().edge_dest(id);
            return 0.5 * (src + dst);
          }),
      EdgePropertySetter(
          "edge_name", [](Edge id) { return fmt::format("Edge {}", id); }));

  KATANA_LOG_VASSERT(result, "AddEdgeProperties returned an error.");

  KATANA_LOG_VASSERT(
      pg->HasEdgeProperty("average"),
      "PropertyGraph must have the average property");

  KATANA_LOG_VASSERT(
      pg->HasEdgeProperty("edge_name"),
      "PropertyGraph must have the edge_name property");

  auto avgs = pg->GetEdgeProperty("average").value();
  auto avgs_array =
      std::static_pointer_cast<arrow::DoubleArray>(avgs->chunk(0));

  auto names = pg->GetEdgeProperty("edge_name").value();
  auto names_array =
      std::static_pointer_cast<arrow::StringArray>(names->chunk(0));

  size_t i = 0;
  auto edges = pg->topology().all_edges();
  for (auto edge_it = edges.begin(); edge_it != edges.end(); ++edge_it, ++i) {
    Node src = pg->topology().edge_source(*edge_it);
    Node dst = pg->topology().edge_dest(*edge_it);
    std::string expected_name = fmt::format("Edge {}", *edge_it);

    KATANA_LOG_VASSERT(
        avgs_array->Value(i) == 0.5 * (src + dst),
        "Incorrect edge average value");

    KATANA_LOG_VASSERT(
        names_array->GetString(i) == expected_name,
        "Incorrect edge name value");
  }
}

int
main() {
  katana::SharedMemSys S;

  TestNodeProps(katana::MakeGrid(3, 4, true));
  TestEdgeProps(katana::MakeGrid(3, 4, true));

  return 0;
}
