#include <arrow/api.h>

#include "katana/BuildGraph.h"
#include "katana/Logging.h"
#include "katana/PropertyGraph.h"
#include "katana/SharedMemSys.h"

using LocalNodeID = uint32_t;

std::unique_ptr<katana::PropertyGraph>
CreateGraph1() {
  katana::PropertyGraphBuilder pgb(2500);
  constexpr int kNumNodes = 100;
  std::string node0_prop_id = "n0";
  std::string edge0_prop_id = "rank";
  using katana::PropertyKey;

  {
    PropertyKey node_pk(
        node0_prop_id, true, false,
        /* Arrow name */ node0_prop_id, katana::ImportDataType::kInt64, false);
    for (LocalNodeID i = 0; i < kNumNodes; ++i) {
      auto id = std::to_string(i);
      bool start_ok = pgb.StartNode(id);
      KATANA_LOG_ASSERT(start_ok);
      pgb.AddValue(
          node0_prop_id, [&]() -> PropertyKey { return node_pk; },
          [i](katana::ImportDataType type, bool is_list) -> katana::ImportData {
            (void)(type);
            (void)(is_list);
            katana::ImportData data(katana::ImportDataType::kInt64, false);
            data.value = static_cast<int64_t>(i);
            return data;
          });
      pgb.FinishNode();
    }
  }

  PropertyKey edge_pk(
      edge0_prop_id, false, true,
      /* Arrow name */ edge0_prop_id, katana::ImportDataType::kInt64, false);
  for (LocalNodeID i = 0; i < kNumNodes; ++i) {
    for (LocalNodeID j = 0; j < kNumNodes; ++j) {
      bool start_ok = pgb.StartEdge(std::to_string(i), std::to_string(j));
      KATANA_LOG_ASSERT(start_ok);
      pgb.AddValue(
          edge0_prop_id, [&]() -> PropertyKey { return edge_pk; },
          [i, j](
              katana::ImportDataType type, bool is_list) -> katana::ImportData {
            (void)(type);
            (void)(is_list);
            katana::ImportData data(katana::ImportDataType::kInt64, false);
            data.value = static_cast<int64_t>(i * j);
            return data;
          });
      pgb.FinishEdge();
    }
  }
  auto components_result = pgb.Finish(false);
  if (!components_result) {
    KATANA_LOG_FATAL(
        "Failed to construct graph: {}", components_result.error());
  }
  auto graph_result =
      katana::ConvertToPropertyGraph(std::move(components_result.value()));
  if (!graph_result) {
    KATANA_LOG_FATAL("Failed to construct graph: {}", graph_result.error());
  }
  return std::move(graph_result.value());
}

std::unique_ptr<katana::PropertyGraph>
CreateGraph2() {
  katana::PropertyGraphBuilder pgb(2500);
  constexpr int kNumNodes = 100;
  std::string node0_prop_id = "n0";
  std::string edge0_prop_id = "rank";
  using katana::PropertyKey;

  {
    PropertyKey node_pk(
        node0_prop_id, true, false,
        /* Arrow name */ node0_prop_id, katana::ImportDataType::kInt64, false);
    for (LocalNodeID i = 0; i < kNumNodes; ++i) {
      auto id = std::to_string(i);
      bool start_ok = pgb.StartNode(id);
      KATANA_LOG_ASSERT(start_ok);
      pgb.AddValue(
          node0_prop_id, [&]() -> PropertyKey { return node_pk; },
          [i](katana::ImportDataType type, bool is_list) -> katana::ImportData {
            (void)(type);
            (void)(is_list);
            katana::ImportData data(katana::ImportDataType::kInt64, false);
            if (i == (kNumNodes - 1)) {
              data.value = static_cast<int64_t>(0);
            } else {
              data.value = static_cast<int64_t>(i);
            }
            return data;
          });
      pgb.FinishNode();
    }
  }

  PropertyKey edge_pk(
      edge0_prop_id, false, true,
      /* Arrow name */ edge0_prop_id, katana::ImportDataType::kInt64, false);
  for (LocalNodeID i = 0; i < kNumNodes; ++i) {
    for (LocalNodeID j = 0; j < kNumNodes; ++j) {
      bool start_ok = pgb.StartEdge(std::to_string(i), std::to_string(j));
      KATANA_LOG_ASSERT(start_ok);
      pgb.AddValue(
          edge0_prop_id, [&]() -> PropertyKey { return edge_pk; },
          [i, j](
              katana::ImportDataType type, bool is_list) -> katana::ImportData {
            (void)(type);
            (void)(is_list);
            katana::ImportData data(katana::ImportDataType::kInt64, false);
            if ((i == (kNumNodes - 1)) && (j == 7)) {
              data.value = static_cast<int64_t>(2 * i * j);
            } else {
              data.value = static_cast<int64_t>(i * j);
            }
            return data;
          });
      pgb.FinishEdge();
    }
  }
  auto components_result = pgb.Finish(false);
  if (!components_result) {
    KATANA_LOG_FATAL(
        "Failed to construct graph: {}", components_result.error());
  }
  auto graph_result =
      katana::ConvertToPropertyGraph(std::move(components_result.value()));
  if (!graph_result) {
    KATANA_LOG_FATAL("Failed to construct graph: {}", graph_result.error());
  }
  return std::move(graph_result.value());
}

std::unique_ptr<katana::PropertyGraph>
CreateGraph3() {
  katana::PropertyGraphBuilder pgb(2500);
  constexpr int kNumNodes = 100;
  std::string node0_prop_id = "n0";
  std::string edge0_prop_id = "rank";
  using katana::PropertyKey;

  {
    PropertyKey node_pk(
        node0_prop_id, true, false,
        /* Arrow name */ node0_prop_id, katana::ImportDataType::kInt64, false);
    for (LocalNodeID i = 0; i < kNumNodes; ++i) {
      auto id = std::to_string(i);
      bool start_ok = pgb.StartNode(id);
      KATANA_LOG_ASSERT(start_ok);
      pgb.AddValue(
          node0_prop_id, [&]() -> PropertyKey { return node_pk; },
          [i](katana::ImportDataType type, bool is_list) -> katana::ImportData {
            (void)(type);
            (void)(is_list);
            katana::ImportData data(katana::ImportDataType::kInt64, false);
            data.value = static_cast<int64_t>(2 * (i / 2));
            return data;
          });
      pgb.FinishNode();
    }
  }

  PropertyKey edge_pk(
      edge0_prop_id, false, true,
      /* Arrow name */ edge0_prop_id, katana::ImportDataType::kInt64, false);
  for (LocalNodeID i = 0; i < kNumNodes; ++i) {
    for (LocalNodeID j = 0; j < kNumNodes; ++j) {
      bool start_ok = pgb.StartEdge(std::to_string(i), std::to_string(j));
      KATANA_LOG_ASSERT(start_ok);
      pgb.AddValue(
          edge0_prop_id, [&]() -> PropertyKey { return edge_pk; },
          [i, j](
              katana::ImportDataType type, bool is_list) -> katana::ImportData {
            (void)(type);
            (void)(is_list);
            katana::ImportData data(katana::ImportDataType::kInt64, false);
            data.value = static_cast<int64_t>(2 * ((i * j) / 2));
            return data;
          });
      pgb.FinishEdge();
    }
  }
  auto components_result = pgb.Finish(false);
  if (!components_result) {
    KATANA_LOG_FATAL(
        "Failed to construct graph: {}", components_result.error());
  }
  auto graph_result =
      katana::ConvertToPropertyGraph(std::move(components_result.value()));
  if (!graph_result) {
    KATANA_LOG_FATAL("Failed to construct graph: {}", graph_result.error());
  }
  return std::move(graph_result.value());
}

int
main() {
  katana::SharedMemSys sys;
  auto g1 = CreateGraph1();
  KATANA_LOG_ASSERT(g1->Equals(g1.get()));
  std::string out1 =
      "Topologies match!\n"
      "NodeEntityTypeManager Diff:\n"
      "entity_type_id_to_atomic_entity_type_ids_ match!\n"
      "atomic_entity_type_id_to_type_name_ match!\n"
      "atomic_type_name_to_entity_type_id_ match!\n"
      "atomic_entity_type_id_to_entity_type_ids_ match!\n"
      "EdgeEntityTypeManager Diff:\n"
      "entity_type_id_to_atomic_entity_type_ids_ match!\n"
      "atomic_entity_type_id_to_type_name_ match!\n"
      "atomic_type_name_to_entity_type_id_ match!\n"
      "atomic_entity_type_id_to_entity_type_ids_ match!\n"
      "node_entity_type_ids Match!\n"
      "edge_entity_type_ids Match!\n"
      "Node property n0              (int64)      matches!\n"
      "Edge property rank            (int64)      matches!\n";
  KATANA_LOG_VASSERT(
      g1->ReportDiff(g1.get()) == out1, "{}{}", g1->ReportDiff(g1.get()), out1);

  auto g2 = CreateGraph2();
  KATANA_LOG_ASSERT(!g1->Equals(g2.get()));
  std::string out2 =
      "Topologies match!\n"
      "NodeEntityTypeManager Diff:\n"
      "entity_type_id_to_atomic_entity_type_ids_ match!\n"
      "atomic_entity_type_id_to_type_name_ match!\n"
      "atomic_type_name_to_entity_type_id_ match!\n"
      "atomic_entity_type_id_to_entity_type_ids_ match!\n"
      "EdgeEntityTypeManager Diff:\n"
      "entity_type_id_to_atomic_entity_type_ids_ match!\n"
      "atomic_entity_type_id_to_type_name_ match!\n"
      "atomic_type_name_to_entity_type_id_ match!\n"
      "atomic_entity_type_id_to_entity_type_ids_ match!\n"
      "node_entity_type_ids Match!\n"
      "edge_entity_type_ids Match!\n"
      "Node property n0              (int64)      differs\n"
      "@@ -99, +99 @@\n"
      "-99\n"
      "+0\n"
      "Edge property rank            (int64)      differs\n"
      "@@ -9907, +9907 @@\n"
      "-693\n"
      "+1386\n";
  KATANA_LOG_VASSERT(
      g1->ReportDiff(g2.get()) == out2, "{}{}", g1->ReportDiff(g2.get()), out2);

  auto g3 = CreateGraph3();
  KATANA_LOG_ASSERT(!g1->Equals(g3.get()));
  std::string out3 =
      "Topologies match!\n"
      "NodeEntityTypeManager Diff:\n"
      "entity_type_id_to_atomic_entity_type_ids_ match!\n"
      "atomic_entity_type_id_to_type_name_ match!\n"
      "atomic_type_name_to_entity_type_id_ match!\n"
      "atomic_entity_type_id_to_entity_type_ids_ match!\n"
      "EdgeEntityTypeManager Diff:\n"
      "entity_type_id_to_atomic_entity_type_ids_ match!\n"
      "atomic_entity_type_id_to_type_name_ match!\n"
      "atomic_type_name_to_entity_type_id_ match!\n"
      "atomic_entity_type_id_to_entity_type_ids_ match!\n"
      "node_entity_type_ids Match!\n"
      "edge_entity_type_ids Match!\n"
      "Node property n0              (int64)      differs\n"
      "@@ -1, +1 @@\n"
      "-1\n"
      "+0\n"
      "@@ -3, +3 @@\n"
      "-3\n"
      "+2\n"
      "@@ -5, +5 @@\n"
      "-5\n"
      "+4\n"
      "@@ -7, +7 @@\n"
      "-7\n"
      "+6\n"
      "@@ -9, +9 @@\n"
      "-9\n"
      "+8\n"
      "@@ -11, +11 @@\n"
      "-11\n"
      "+10\n"
      "@@ -13, +13 @@\n"
      "-13\n"
      "+12\n"
      "@@ -15, +15 @@\n"
      "-...\n"
      "Edge property rank            (int64)      differs\n"
      "@@ -101, +101 @@\n"
      "-1\n"
      "+0\n"
      "@@ -103, +103 @@\n"
      "-3\n"
      "+2\n"
      "@@ -105, +105 @@\n"
      "-5\n"
      "+4\n"
      "@@ -107, +107 @@\n"
      "-7\n"
      "+6\n"
      "@@ -109, +109 @@\n"
      "-9\n"
      "+8\n"
      "@@ -111, +111 @@\n"
      "-11\n"
      "+10\n"
      "@@ -113, +113 @@\n"
      "-...\n";
  KATANA_LOG_VASSERT(
      g1->ReportDiff(g3.get()) == out3, "{}{}", g1->ReportDiff(g3.get()), out3);

  return 0;
}
