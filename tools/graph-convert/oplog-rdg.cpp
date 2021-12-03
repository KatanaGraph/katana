#include "katana/BuildGraph.h"
#include "katana/SharedMemSys.h"

constexpr uint64_t kNumNodes = 100;

class LogPlay {
  katana::PropertyGraphBuilder pgb;

public:
  LogPlay() : pgb(25000) {}

  bool StartNode(const std::string& id) { return pgb.StartNode(id); }
  void FinishNode() { pgb.FinishNode(); }
  bool StartEdge(const std::string& source, const std::string& target) {
    return pgb.StartEdge(source, target);
  }
  void FinishEdge() { pgb.FinishEdge(); }
  void AddPropValue(
      const std::string& id,
      std::function<katana::PropertyKey()> ProcessElement,
      std::function<katana::ImportData(katana::ImportDataType, bool)>
          ResolveValue) {
    pgb.AddValue(id, ProcessElement, ResolveValue);
  }
  void CreateRDG(tsuba::TxnContext* txn_ctx) {
    auto uri_res = katana::Uri::MakeRand("/tmp/oplog");
    KATANA_LOG_ASSERT(uri_res);
    std::string dest_dir(uri_res.value().string());
    auto components_result = pgb.Finish();
    if (!components_result) {
      KATANA_LOG_FATAL(
          "Failed to construct graph: {}", components_result.error());
    }
    if (auto r = WritePropertyGraph(
            std::move(components_result.value()), dest_dir, txn_ctx);
        !r) {
      KATANA_LOG_FATAL("Failed to write graph: {}", r.error());
    }
    fmt::print("RDG written to {}\n", dest_dir);
  }
};

void
ReadLog(tsuba::TxnContext* txn_ctx) {
  LogPlay lp;

  std::string prop_id = "n0";
  {
    katana::PropertyKey node_pk(
        prop_id, true, false,
        /* Arrow name */ prop_id, katana::ImportDataType::kInt64, false);
    for (uint64_t i = 0; i < kNumNodes; ++i) {
      auto id = std::to_string(i);
      bool start_ok = lp.StartNode(id);
      KATANA_LOG_ASSERT(start_ok);
      lp.AddPropValue(
          prop_id, [&]() -> katana::PropertyKey { return node_pk; },
          [i](katana::ImportDataType type, bool is_list) -> katana::ImportData {
            (void)(type);
            (void)(is_list);
            katana::ImportData data(katana::ImportDataType::kInt64, false);
            data.value = (int64_t)i;
            return data;
          });
      lp.FinishNode();
    }
  }

  prop_id = "rank";
  katana::PropertyKey edge_pk(
      prop_id, false, true,
      /* Arrow name */ prop_id, katana::ImportDataType::kInt64, false);
  for (uint64_t i = 0; i < kNumNodes; ++i) {
    for (uint64_t j = 0; j < kNumNodes; ++j) {
      bool start_ok = lp.StartEdge(std::to_string(i), std::to_string(j));
      KATANA_LOG_ASSERT(start_ok);
      lp.AddPropValue(
          prop_id, [&]() -> katana::PropertyKey { return edge_pk; },
          [i, j](
              katana::ImportDataType type, bool is_list) -> katana::ImportData {
            (void)(type);
            (void)(is_list);
            katana::ImportData data(katana::ImportDataType::kInt64, false);
            data.value = (int64_t)(i * j);
            return data;
          });
      lp.FinishEdge();
    }
  }
  lp.CreateRDG(txn_ctx);
}

int
main() {  //int argc, char* argv[]) {
  katana::SharedMemSys sys;

  tsuba::TxnContext txn_ctx;
  ReadLog(&txn_ctx);

  return 0;
}
