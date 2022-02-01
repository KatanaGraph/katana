#ifndef KATANA_LIBTSUBA_TESTRDG_H_
#define KATANA_LIBTSUBA_TESTRDG_H_

#include <string>

#include "katana/Logging.h"
#include "katana/RDG.h"
#include "katana/RDGManifest.h"
#include "katana/Result.h"
#include "katana/URI.h"

katana::Result<std::string>
WriteRDG(
    katana::RDG&& rdg_, katana::EntityTypeManager node_entity_type_manager,
    katana::EntityTypeManager edge_entity_type_manager,
    std::string tmp_rdg_dir) {
  std::string command_line;

  // Store graph. If there is a new storage format then storing it is enough to bump the version up.
  KATANA_LOG_WARN("writing graph at temp file {}", tmp_rdg_dir);

  if (auto res = katana::Create(tmp_rdg_dir); !res) {
    return res.error();
  }

  katana::RDGManifest manifest =
      KATANA_CHECKED(katana::FindManifest(tmp_rdg_dir));
  auto open_res = katana::Open(std::move(manifest), katana::kReadWrite);
  if (!open_res) {
    return open_res.error();
  }
  auto new_file = std::make_unique<katana::RDGFile>(open_res.value());

  katana::TxnContext txn_ctx;
  auto res = rdg_.Store(
      *new_file, command_line,
      katana::RDG::RDGVersioningPolicy::IncrementVersion, nullptr, nullptr,
      node_entity_type_manager, edge_entity_type_manager, &txn_ctx);

  if (!res) {
    return res.error();
  }
  return tmp_rdg_dir;
}

katana::Result<std::string>
WriteRDG(
    katana::RDG&& rdg_, katana::EntityTypeManager node_entity_type_manager,
    katana::EntityTypeManager edge_entity_type_manager) {
  auto uri_res = katana::Uri::MakeRand("/tmp/propertyfilegraph");
  KATANA_LOG_ASSERT(uri_res);
  std::string tmp_rdg_dir(uri_res.value().path());  // path() because local

  return WriteRDG(
      std::move(rdg_), std::move(node_entity_type_manager),
      std::move(edge_entity_type_manager), tmp_rdg_dir);
}

katana::Result<std::string>
WriteRDG(katana::RDG&& rdg_) {
  return WriteRDG(
      std::move(rdg_), KATANA_CHECKED(rdg_.node_entity_type_manager()),
      KATANA_CHECKED(rdg_.edge_entity_type_manager()));
}

katana::Result<std::string>
WriteRDG(katana::RDG&& rdg_, std::string out_dir) {
  return WriteRDG(
      std::move(rdg_), KATANA_CHECKED(rdg_.node_entity_type_manager()),
      KATANA_CHECKED(rdg_.edge_entity_type_manager()), out_dir);
}

katana::Result<katana::RDG>
LoadRDG(const std::string& rdg_name) {
  katana::RDGManifest manifest = KATANA_CHECKED(katana::FindManifest(rdg_name));
  katana::RDGFile rdg_file{
      KATANA_CHECKED(katana::Open(std::move(manifest), katana::kReadWrite))};
  katana::RDG rdg =
      KATANA_CHECKED(katana::RDG::Make(rdg_file, katana::RDGLoadOptions()));

  return katana::RDG(std::move(rdg));
}

#endif
