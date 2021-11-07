#include "katana/EntityTypeManager.h"
#include "katana/Logging.h"

void
CreateEntityTypeIDs() {
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

void
ValidateConstructor() {
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

  katana::EntityTypeIDToAtomicTypeNameMap name_map(
      mgr.GetEntityTypeIDToAtomicTypeNameMap());
  katana::EntityTypeIDToSetOfEntityTypeIDsMap id_map(
      mgr.GetEntityTypeIDToAtomicEntityTypeIDs());

  KATANA_LOG_ASSERT(name_map == mgr.GetEntityTypeIDToAtomicTypeNameMap());
  KATANA_LOG_ASSERT(id_map == mgr.GetEntityTypeIDToAtomicEntityTypeIDs());

  katana::EntityTypeManager mgr_copy(std::move(name_map), std::move(id_map));

  if (!mgr.Equals(mgr_copy)) {
    KATANA_LOG_WARN("{}", mgr.ReportDiff(mgr_copy));
    KATANA_LOG_ASSERT(false);
  }
}

int
main() {
  CreateEntityTypeIDs();
  ValidateConstructor();
}
