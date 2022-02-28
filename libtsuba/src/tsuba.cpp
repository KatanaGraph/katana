#include "katana/tsuba.h"

#include "GlobalState.h"
#include "RDGHandleImpl.h"
#include "RDGPartHeader.h"
#include "katana/CommBackend.h"
#include "katana/Env.h"
#include "katana/ErrorCode.h"
#include "katana/FileView.h"
#include "katana/Plugin.h"
#include "katana/Signals.h"
#include "katana/URI.h"
#include "katana/file.h"

namespace {

katana::NullCommBackend default_comm_backend;

katana::Result<std::vector<std::string>>
FileList(const std::string& dir) {
  std::vector<std::string> files;
  auto list_fut = katana::FileListAsync(dir, &files);
  KATANA_LOG_ASSERT(list_fut.valid());

  KATANA_CHECKED(list_fut.get());
  return files;
}

katana::Result<katana::URI>
FindAnyManifestForLatestVersion(const katana::URI& name) {
  KATANA_LOG_DEBUG_ASSERT(!katana::RDGManifest::IsManifestUri(name));
  std::vector<std::string> file_list = KATANA_CHECKED(FileList(name.string()));

  uint64_t version = 0;
  std::string found_manifest;
  for (const std::string& file : file_list) {
    if (auto res = katana::RDGManifest::ParseVersionFromName(file); res) {
      uint64_t new_version = res.value();
      if (new_version >= version) {
        version = new_version;
        found_manifest = file;
      }
    }
  }
  if (found_manifest.empty()) {
    return KATANA_ERROR(
        katana::ErrorCode::NotFound,
        "failed: could not find manifest file in {}", name);
  }
  return name.Join(found_manifest);
}

}  // namespace

katana::Result<katana::RDGManifest>
katana::FindManifest(const std::string& rdg_name) {
  katana::URI uri = KATANA_CHECKED(katana::URI::Make(rdg_name));

  if (RDGManifest::IsManifestUri(uri)) {
    RDGManifest manifest = KATANA_CHECKED(katana::RDGManifest::Make(uri));
    return manifest;
  }

  auto latest_uri = FindAnyManifestForLatestVersion(uri);
  if (!latest_uri) {
    return KATANA_ERROR(
        ErrorCode::InvalidArgument, "failed to find latest RDGManifest at {}",
        uri.string());
  }

  RDGManifest manifest =
      KATANA_CHECKED(katana::RDGManifest::Make(latest_uri.value()));
  return manifest;
}

katana::Result<katana::RDGManifest>
katana::FindManifest(const std::string& rdg_name, katana::TxnContext* txn_ctx) {
  katana::URI uri = KATANA_CHECKED(katana::URI::Make(rdg_name));
  if (RDGManifest::IsManifestUri(uri)) {
    uri = uri.DirName();
  }

  if (txn_ctx && txn_ctx->ManifestCached(uri)) {
    return txn_ctx->ManifestInfo(uri).rdg_manifest;
  } else {
    return KATANA_CHECKED(katana::FindManifest(rdg_name));
  }
}

katana::Result<katana::RDGHandle>
katana::Open(RDGManifest rdg_manifest, uint32_t flags) {
  if (!OpenFlagsValid(flags)) {
    return KATANA_ERROR(
        ErrorCode::InvalidArgument, "invalid value for flags ({:#x})", flags);
  }

  return RDGHandle{.impl_ = new RDGHandleImpl(flags, std::move(rdg_manifest))};
}

katana::Result<void>
katana::Close(RDGHandle handle) {
  delete handle.impl_;
  return katana::ResultSuccess();
}

katana::Result<void>
katana::Create(const std::string& name) {
  katana::URI uri = KATANA_CHECKED(katana::URI::Make(name));

  KATANA_LOG_DEBUG_ASSERT(!RDGManifest::IsManifestUri(uri));
  // the default construction is the empty RDG
  katana::RDGManifest manifest{};

  katana::CommBackend* comm = Comm();
  if (comm->Rank == 0) {
    std::string s = manifest.ToJsonString();
    if (auto res = katana::FileStore(
            katana::RDGManifest::FileName(
                uri, katana::kDefaultRDGViewType, manifest.version())
                .string(),
            reinterpret_cast<const uint8_t*>(s.data()), s.size());
        !res) {
      comm->NotifyFailure();
      return res.error().WithContext(
          "failed to store RDG file: {}", uri.string());
    }
  }
  Comm()->Barrier();

  return katana::ResultSuccess();
}

namespace {

katana::Result<void>
ParseManifestName(
    const std::string& filename, std::string* type,
    std::vector<std::string>* args, uint64_t* version) {
  *type = KATANA_CHECKED(katana::RDGManifest::ParseViewNameFromName(filename));
  *args = KATANA_CHECKED(katana::RDGManifest::ParseViewArgsFromName(filename));
  *version =
      KATANA_CHECKED(katana::RDGManifest::ParseVersionFromName(filename));
  return katana::ResultSuccess();
}

}  // namespace

katana::Result<std::pair<uint64_t, std::vector<katana::RDGView>>>
katana::ListViewsOfVersion(
    const std::string& rdg_dir, std::optional<uint64_t> version) {
  auto rdg_uri = KATANA_CHECKED(katana::URI::Make(rdg_dir));
  std::vector<std::string> files = KATANA_CHECKED(FileList(rdg_uri.string()));

  std::vector<katana::RDGView> views_found;
  uint64_t latest_version = 0;
  bool acceptable_version_found = false;
  for (const std::string& file : files) {
    std::string view_type;
    std::vector<std::string> view_args;
    uint64_t view_version{};

    if (!ParseManifestName(file, &view_type, &view_args, &view_version)) {
      continue;
    }

    // if a specific version was requested, only return views for that version
    if (version && view_version != version.value()) {
      continue;
    }
    acceptable_version_found = true;

    // otherwise only return views for the latest version
    if (view_version > latest_version) {
      // we only keep views from the latest
      latest_version = view_version;
      views_found.clear();
    }
    if (view_version < latest_version) {
      continue;
    }

    katana::URI manifest_path = rdg_uri.Join(file);

    const RDGManifest& manifest =
        KATANA_CHECKED(RDGManifest::Make(manifest_path));

    if (manifest.num_hosts() == 0) {
      // empty sentinal; not a valid view
      continue;
    }

    views_found.emplace_back(katana::RDGView{
        .view_type = view_type,
        .view_args = fmt::format("{}", fmt::join(view_args, "-")),
        .view_path = manifest_path.string(),
        .num_partitions = manifest.num_hosts(),
        .policy_id = manifest.policy_id(),
        .transpose = manifest.transpose(),
    });
  }

  if (!acceptable_version_found) {
    if (version) {
      return KATANA_ERROR(
          katana::ErrorCode::NotFound, "no views found for requested version");
    } else {
      return KATANA_ERROR(
          katana::ErrorCode::NotFound, "no manifest files found in directory");
    }
  }

  return std::make_pair(latest_version, views_found);
}

katana::Result<std::pair<uint64_t, std::vector<katana::RDGView>>>
katana::ListAvailableViews(
    const std::string& rdg_dir, std::optional<uint64_t> version) {
  return ListViewsOfVersion(rdg_dir, version);
}

katana::Result<std::vector<std::pair<katana::URI, katana::URI>>>
katana::CreateSrcDestFromViewsForCopy(
    const std::string& src_dir, const std::string& dst_dir, uint64_t version) {
  std::vector<std::pair<katana::URI, katana::URI>> src_dst_files;

  // List out all the files in a given view
  auto rdg_views = KATANA_CHECKED(ListViewsOfVersion(src_dir, version));
  for (const auto& rdg_view : rdg_views.second) {
    auto rdg_view_uri = KATANA_CHECKED(katana::URI::Make(rdg_view.view_path));
    auto rdg_manifest_res = katana::RDGManifest::Make(rdg_view_uri);
    if (!rdg_manifest_res) {
      KATANA_LOG_WARN(
          "not a valid manifest file: {}, {}", rdg_view_uri.string(),
          rdg_manifest_res.error());
      continue;
    }

    auto rdg_manifest = std::move(rdg_manifest_res.value());

    auto fnames = KATANA_CHECKED(rdg_manifest.FileNames());
    for (const auto& fname : fnames) {
      auto src_file_path = katana::URI::JoinPath(src_dir, fname);
      auto src_file_uri = KATANA_CHECKED(katana::URI::Make(src_file_path));

      // Skip manifests for now, will be handled at the end
      if (katana::RDGManifest::IsManifestUri(src_file_uri)) {
        continue;
      }

      // Check to see if we have a partition file
      // If we have a partition file, the dst path should be based on PartitionFileName using rdg manifest info
      // We're batching this now because we want to rely on having the RDG manifest file in case things
      // change in the future.
      katana::URI dst_file_uri;
      if (katana::RDGPartHeader::IsPartitionFileUri(src_file_uri)) {
        auto host_id =
            KATANA_CHECKED(katana::RDGPartHeader::ParseHostFromPartitionFile(
                src_file_uri.BaseName()));
        auto dst_dir_uri = KATANA_CHECKED(katana::URI::Make(dst_dir));
        dst_file_uri = katana::RDGManifest::PartitionFileName(
            rdg_manifest.view_specifier(), dst_dir_uri, host_id,
            1 /* version */);
      } else {
        auto dst_file_path = katana::URI::JoinPath(dst_dir, fname);
        dst_file_uri = KATANA_CHECKED(katana::URI::Make(dst_file_path));
      }
      src_dst_files.emplace_back(std::make_pair(src_file_uri, dst_file_uri));
    }

    // We add the manifest file to the vector
    auto rdg_manifest_uri = rdg_manifest.FileName();

    // Reset the version
    rdg_manifest.set_version(1);
    rdg_manifest.set_prev_version(1);

    auto dst_rdg_manifest_path =
        katana::URI::JoinPath(dst_dir, rdg_manifest.FileName().BaseName());
    auto dst_rdg_manifest_uri =
        KATANA_CHECKED(katana::URI::Make(dst_rdg_manifest_path));
    src_dst_files.emplace_back(
        std::make_pair(rdg_manifest_uri, dst_rdg_manifest_uri));
  }
  return src_dst_files;
}

katana::Result<void>
katana::CopyRDG(
    std::vector<std::pair<katana::URI, katana::URI>> src_dst_files) {
  // TODO(vkarthik): add do_all loop
  std::vector<uint64_t> manifest_uri_idxs;
  for (uint64_t i = 0; i < src_dst_files.size(); i++) {
    auto [src_file_uri, dst_file_uri] = src_dst_files[i];
    // We save the names of all the manifest files and we write them out at the end.
    if (katana::RDGManifest::IsManifestUri(src_file_uri)) {
      manifest_uri_idxs.push_back(i);
      continue;
    }
    katana::FileView fv;
    KATANA_CHECKED(fv.Bind(src_file_uri.string(), true));
    KATANA_CHECKED(
        katana::FileStore(dst_file_uri.string(), fv.ptr<char>(), fv.size()));
  }

  // Process all the manifest files, write them out.
  // We want to write this last so that we know whether a write fully finished or not.
  for (auto idx : manifest_uri_idxs) {
    auto [src_file_uri, dst_file_uri] = src_dst_files[idx];
    auto rdg_manifest = KATANA_CHECKED(katana::RDGManifest::Make(src_file_uri));
    // These are hard-coded for now. Will what we copy always be version 1?
    // Should we clear the lineage as well?
    // Reset the version
    rdg_manifest.set_version(1);
    rdg_manifest.set_prev_version(1);

    auto rdg_manifest_json = rdg_manifest.ToJsonString();
    KATANA_CHECKED(katana::FileStore(
        dst_file_uri.string(),
        reinterpret_cast<const uint8_t*>(rdg_manifest_json.data()),
        rdg_manifest_json.size()));
  }
  return katana::ResultSuccess();
}

katana::Result<void>
katana::WriteRDGPartHeader(
    std::vector<katana::RDGPropInfo> node_properties,
    std::vector<katana::RDGPropInfo> edge_properties,
    katana::EntityTypeManager& node_entity_type_manager,
    katana::EntityTypeManager& edge_entity_type_manager,
    const std::string& node_entity_type_id_array_path,
    const std::string& edge_entity_type_id_array_path, uint64_t num_nodes,
    uint64_t num_edges, const std::string& topology_path,
    const std::string& rdg_dir) {
  auto manifest = RDGManifest();
  katana::URI rdg_dir_uri = KATANA_CHECKED(katana::URI::Make(rdg_dir));
  manifest.set_dir(rdg_dir_uri);
  manifest.set_viewtype(katana::kDefaultRDGViewType);
  auto part_header = katana::RDGPartHeader();

  // Setup partition metadata, for now assume only one partition
  katana::PartitionMetadata part_meta;
  part_meta.num_global_nodes_ = num_nodes;
  part_meta.max_global_node_id_ = num_nodes - 1;
  part_meta.num_global_edges_ = num_edges;
  part_meta.num_edges_ = num_edges;
  part_meta.num_nodes_ = num_nodes;
  part_meta.num_owned_ = num_nodes;
  part_meta.is_outgoing_edge_cut_ = true;

  // Create vector that is needed by part_header for prop_info, do this for both node and edges
  std::vector<katana::PropStorageInfo> node_props;
  node_props.reserve(node_properties.size());
  for (auto rdg_prop_info : node_properties) {
    node_props.emplace_back(katana::PropStorageInfo(
        rdg_prop_info.property_name, rdg_prop_info.property_path));
  }

  std::vector<katana::PropStorageInfo> edge_props;
  edge_props.reserve(edge_properties.size());
  for (auto rdg_prop_info : edge_properties) {
    edge_props.emplace_back(katana::PropStorageInfo(
        rdg_prop_info.property_name, rdg_prop_info.property_path));
  }

  // Set the node and edge prop info lists
  part_header.set_node_prop_info_list(std::move(node_props));
  part_header.set_edge_prop_info_list(std::move(edge_props));

  // Set the entity type managers for nodes and edges
  KATANA_CHECKED(
      part_header.StoreNodeEntityTypeManager(node_entity_type_manager));
  KATANA_CHECKED(
      part_header.StoreEdgeEntityTypeManager(edge_entity_type_manager));

  // Set the paths for the entity type id arrays
  part_header.set_node_entity_type_id_array_path(
      node_entity_type_id_array_path);
  part_header.set_edge_entity_type_id_array_path(
      edge_entity_type_id_array_path);

  // Set the topology metadata
  PartitionTopologyMetadataEntry* topology_metadata_entry =
      part_header.MakePartitionTopologyMetadataEntry(topology_path);
  topology_metadata_entry->FillCSRMetadataEntry(num_nodes, num_edges);

  // Set storage format version to the latest one
  // This will also prevent bumping up storage versions and leaving the dask
  // import code in a stale state. Especially if there are large changes.
  part_header.update_storage_format_version();

  // all rdgs stored while the unstable rdg storage format flag is set
  // are considered to be in the unstable rdg storage format
  if (KATANA_EXPERIMENTAL_ENABLED(UnstableRDGStorageFormat)) {
    part_header.set_unstable_storage_format();
  }

  // Set partition metadata
  part_header.set_metadata(part_meta);

  // Write out the part_header
  auto policy = katana::RDG::RDGVersioningPolicy::IncrementVersion;
  katana::RDGHandle handle =
      KATANA_CHECKED(katana::Open(std::move(manifest), katana::kReadWrite));
  KATANA_CHECKED(part_header.Write(handle, policy));
  return katana::ResultSuccess();
}

katana::Result<void>
katana::WriteRDGManifest(const std::string& rdg_dir, uint32_t num_hosts) {
  auto manifest = RDGManifest();
  katana::URI rdg_dir_uri = KATANA_CHECKED(katana::URI::Make(rdg_dir));
  manifest.set_dir(rdg_dir_uri);
  manifest.set_viewtype(katana::kDefaultRDGViewType);
  manifest.set_version(1);
  manifest.set_prev_version(1);
  manifest.set_num_hosts(num_hosts);

  // Write out the manifest file
  // Using view_specifier in case something ever changes in the future where out-of-core import
  // will be able to partition a graph, etc.
  auto manifest_json = manifest.ToJsonString();
  KATANA_CHECKED(katana::FileStore(
      katana::RDGManifest::FileName(
          rdg_dir_uri, manifest.view_specifier(), manifest.version())
          .string(),
      reinterpret_cast<const uint8_t*>(manifest_json.data()),
      manifest_json.size()));
  return katana::ResultSuccess();
}

/// Create a file name for the default CSR topology
katana::URI
katana::MakeTopologyFileName(katana::RDGHandle handle) {
  return GetRDGDir(handle).RandFile("topology");
}

katana::URI
katana::MakeNodeEntityTypeIDArrayFileName(katana::RDGHandle handle) {
  return GetRDGDir(handle).RandFile("node_entity_type_id_array");
}

katana::URI
katana::MakeEdgeEntityTypeIDArrayFileName(katana::RDGHandle handle) {
  return GetRDGDir(handle).RandFile("edge_entity_type_id_array");
}

katana::URI
katana::GetRDGDir(katana::RDGHandle handle) {
  return handle.impl_->rdg_manifest().dir();
}

katana::Result<void>
katana::InitTsuba(katana::CommBackend* comm) {
  katana::InitSignalHandlers();
  return GlobalState::Init(comm);
}

katana::Result<void>
katana::InitTsuba() {
  return InitTsuba(&default_comm_backend);
}

katana::Result<void>
katana::FiniTsuba() {
  auto r = GlobalState::Fini();
  return r;
}
