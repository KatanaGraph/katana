#ifndef KATANA_LIBTSUBA_TESTRDG_H_
#define KATANA_LIBTSUBA_TESTRDG_H_

#include <string>

#include "katana/Logging.h"
#include "katana/Result.h"
#include "katana/URI.h"
#include "tsuba/RDG.h"
#include "tsuba/RDGManifest.h"

katana::Result<std::string>
WriteRDG(
    tsuba::RDG&& rdg_, katana::EntityTypeManager node_entity_type_manager,
    katana::EntityTypeManager edge_entity_type_manager) {
  auto uri_res = katana::Uri::MakeRand("/tmp/propertyfilegraph");
  KATANA_LOG_ASSERT(uri_res);
  std::string tmp_rdg_dir(uri_res.value().path());  // path() because local
  std::string command_line;

  // Store graph. If there is a new storage format then storing it is enough to bump the version up.
  KATANA_LOG_WARN("writing graph at temp file {}", tmp_rdg_dir);

  if (auto res = tsuba::Create(tmp_rdg_dir); !res) {
    return res.error();
  }

  tsuba::RDGManifest manifest =
      KATANA_CHECKED(tsuba::FindManifest(tmp_rdg_dir));
  auto open_res = tsuba::Open(std::move(manifest), tsuba::kReadWrite);
  if (!open_res) {
    return open_res.error();
  }
  auto new_file = std::make_unique<tsuba::RDGFile>(open_res.value());

  auto res = rdg_.Store(
      *new_file, command_line,
      tsuba::RDG::RDGVersioningPolicy::IncrementVersion, nullptr, nullptr,
      node_entity_type_manager, edge_entity_type_manager);

  if (!res) {
    return res.error();
  }
  return tmp_rdg_dir;
}

katana::Result<std::string>
WriteRDG(tsuba::RDG&& rdg_) {
  return WriteRDG(
      std::move(rdg_), KATANA_CHECKED(rdg_.node_entity_type_manager()),
      KATANA_CHECKED(rdg_.edge_entity_type_manager()));
}

katana::Result<tsuba::RDG>
LoadRDG(const std::string& rdg_name) {
  tsuba::RDGManifest manifest = KATANA_CHECKED(tsuba::FindManifest(rdg_name));
  tsuba::RDGFile rdg_file{
      KATANA_CHECKED(tsuba::Open(std::move(manifest), tsuba::kReadWrite))};
  tsuba::RDG rdg =
      KATANA_CHECKED(tsuba::RDG::Make(rdg_file, tsuba::RDGLoadOptions()));

  return tsuba::RDG(std::move(rdg));
}

#endif
