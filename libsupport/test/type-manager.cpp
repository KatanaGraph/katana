#include "katana/EntityTypeManager.h"
#include "katana/Logging.h"

int
main() {
  std::vector<katana::TypeNameSet> tnss = {
      {"alice"},
      {"baker"},
      {"alice", "baker"},
      {"charlie"},
      {"david", "eleanor"}};
  std::vector<katana::TypeNameSet> check = {
      {},          {"alice"}, {"baker"},   {"alice", "baker"},
      {"charlie"}, {"david"}, {"eleanor"}, {"david", "eleanor"}};
  katana::EntityTypeManager mgr;
  for (const auto& tns : tnss) {
    auto res = mgr.GetOrAddNonAtomicEntityTypeFromStrings(tns);
    KATANA_LOG_ASSERT(res);
  }
  auto num_entities = mgr.GetNumEntityTypes();
  for (size_t i = 0; i < mgr.GetNumEntityTypes(); ++i) {
    auto res = mgr.EntityTypeToTypeNameSet(i);
    KATANA_LOG_ASSERT(res);
    auto tns = res.value();
    std::string empty = "**empty**";
    KATANA_LOG_VASSERT(
        tns == check[i], "i={} tns[i] ({}) check[i] ({})", i, tns, check[i]);
  }
  katana::TypeNameSet new_tns({"new", "one"});
  auto res = mgr.GetOrAddNonAtomicEntityTypeFromStrings(new_tns);
  KATANA_LOG_ASSERT(res);
  KATANA_LOG_ASSERT(res.value() >= num_entities);

  fmt::print("{}", mgr.PrintEntityTypes());
}
