#include "galois/BuildGraph.h"

constexpr uint64_t kNumNodes = 100;

class LogPlay {
  galois::PropertyGraphBuilder pgb;

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
      std::function<galois::PropertyKey()> ProcessElement,
      std::function<galois::ImportData(galois::ImportDataType, bool)>
          ResolveValue) {
    pgb.AddValue(id, ProcessElement, ResolveValue);
  }
  void CreateRDG() {
    auto uri_res = galois::Uri::MakeRand("/tmp/oplog");
    GALOIS_LOG_ASSERT(uri_res);
    std::string dest_dir(uri_res.value().string());
    WritePropertyGraph(pgb.Finish(), dest_dir);
    fmt::print("RDG written to {}\n", dest_dir);
  }
};

void
ReadLog() {
  LogPlay lp;

  std::string prop_id = "n0";
  {
    galois::PropertyKey node_pk(
        prop_id, true, false,
        /* Arrow name */ prop_id, galois::ImportDataType::kInt64, false);
    for (uint64_t i = 0; i < kNumNodes; ++i) {
      auto id = std::to_string(i);
      bool start_ok = lp.StartNode(id);
      GALOIS_LOG_ASSERT(start_ok);
      lp.AddPropValue(
          prop_id, [&]() -> galois::PropertyKey { return node_pk; },
          [i](galois::ImportDataType type, bool is_list) -> galois::ImportData {
            (void)(type);
            (void)(is_list);
            galois::ImportData data(galois::ImportDataType::kInt64, false);
            data.value = (int64_t)i;
            return data;
          });
      lp.FinishNode();
    }
  }

  prop_id = "rank";
  galois::PropertyKey edge_pk(
      prop_id, false, true,
      /* Arrow name */ prop_id, galois::ImportDataType::kInt64, false);
  for (uint64_t i = 0; i < kNumNodes; ++i) {
    for (uint64_t j = 0; j < kNumNodes; ++j) {
      bool start_ok = lp.StartEdge(std::to_string(i), std::to_string(j));
      GALOIS_LOG_ASSERT(start_ok);
      lp.AddPropValue(
          prop_id, [&]() -> galois::PropertyKey { return edge_pk; },
          [i, j](
              galois::ImportDataType type, bool is_list) -> galois::ImportData {
            (void)(type);
            (void)(is_list);
            galois::ImportData data(galois::ImportDataType::kInt64, false);
            data.value = (int64_t)(i * j);
            return data;
          });
      lp.FinishEdge();
    }
  }
  lp.CreateRDG();
}

int
main() {  //int argc, char* argv[]) {
  galois::SharedMemSys sys;

  ReadLog();

  return 0;
}
