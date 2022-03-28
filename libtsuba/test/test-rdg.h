#ifndef KATANA_LIBTSUBA_TESTRDG_H_
#define KATANA_LIBTSUBA_TESTRDG_H_

#include <filesystem>
#include <string>

#include "katana/ErrorCode.h"
#include "katana/Logging.h"
#include "katana/RDG.h"
#include "katana/RDGManifest.h"
#include "katana/Result.h"
#include "katana/URI.h"

katana::Result<katana::URI>
WriteRDG(
    katana::RDG&& rdg_, katana::EntityTypeManager node_entity_type_manager,
    katana::EntityTypeManager edge_entity_type_manager,
    const katana::URI& tmp_rdg_dir) {
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

katana::Result<katana::URI>
WriteRDG(
    katana::RDG&& rdg_, katana::EntityTypeManager node_entity_type_manager,
    katana::EntityTypeManager edge_entity_type_manager) {
  auto uri_res = katana::URI::MakeRand("/tmp/propertyfilegraph");
  KATANA_LOG_ASSERT(uri_res);
  katana::URI uri = uri_res.value();

  return WriteRDG(
      std::move(rdg_), std::move(node_entity_type_manager),
      std::move(edge_entity_type_manager), uri);
}

katana::Result<katana::URI>
WriteRDG(katana::RDG&& rdg_) {
  return WriteRDG(
      std::move(rdg_), KATANA_CHECKED(rdg_.node_entity_type_manager()),
      KATANA_CHECKED(rdg_.edge_entity_type_manager()));
}

katana::Result<katana::URI>
WriteRDG(katana::RDG&& rdg_, const katana::URI& out_dir) {
  return WriteRDG(
      std::move(rdg_), KATANA_CHECKED(rdg_.node_entity_type_manager()),
      KATANA_CHECKED(rdg_.edge_entity_type_manager()), out_dir);
}

katana::Result<katana::RDG>
LoadRDG(const katana::URI& rdg_dir) {
  KATANA_LOG_WARN("Loading RDG at location {}", rdg_dir);
  katana::RDGManifest manifest = KATANA_CHECKED(katana::FindManifest(rdg_dir));
  katana::RDGFile rdg_file{
      KATANA_CHECKED(katana::Open(std::move(manifest), katana::kReadWrite))};
  katana::RDG rdg =
      KATANA_CHECKED(katana::RDG::Make(rdg_file, katana::RDGLoadOptions()));

  return katana::RDG(std::move(rdg));
}

katana::Result<std::string>
find_file(const std::string& search_path, const std::string& substring) {
  KATANA_LOG_VASSERT(
      search_path.find("file://") == std::string::npos,
      "Function cannot handle paths with the file:// prefix");

  KATANA_LOG_DEBUG("finding file matching {}", substring);
  const std::filesystem::directory_iterator end;
  try {
    for (std::filesystem::directory_iterator iter{search_path}; iter != end;
         iter++) {
      const std::string file_name = iter->path().filename().string();
      if (std::filesystem::is_regular_file(*iter)) {
        if (file_name.find(substring) != std::string::npos) {
          return (iter->path().string());
        }
      }
    }
  } catch (std::exception&) {
  }
  return KATANA_ERROR(
      katana::ErrorCode::InvalidArgument,
      "Unable to find file in in {} containing substring {}", search_path,
      substring);
}

#endif
