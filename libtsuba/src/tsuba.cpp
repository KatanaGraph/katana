#include "tsuba/tsuba.h"

#include "GlobalState.h"
#include "RDGHandleImpl.h"
#include "RDGPartHeader.h"
#include "katana/CommBackend.h"
#include "katana/Env.h"
#include "katana/Plugin.h"
#include "katana/Signals.h"
#include "tsuba/Errors.h"
#include "tsuba/FileView.h"
#include "tsuba/file.h"

namespace {

katana::NullCommBackend default_comm_backend;

katana::Result<std::vector<std::string>>
FileList(const std::string& dir) {
  std::vector<std::string> files;
  auto list_fut = tsuba::FileListAsync(dir, &files);
  KATANA_LOG_ASSERT(list_fut.valid());

  KATANA_CHECKED(list_fut.get());
  return files;
}

katana::Result<katana::Uri>
FindAnyManifestForLatestVersion(const katana::Uri& name) {
  KATANA_LOG_DEBUG_ASSERT(!tsuba::RDGManifest::IsManifestUri(name));
  std::vector<std::string> file_list = KATANA_CHECKED(FileList(name.string()));

  uint64_t version = 0;
  std::string found_manifest;
  for (const std::string& file : file_list) {
    if (auto res = tsuba::RDGManifest::ParseVersionFromName(file); res) {
      uint64_t new_version = res.value();
      if (new_version >= version) {
        version = new_version;
        found_manifest = file;
      }
    }
  }
  if (found_manifest.empty()) {
    return KATANA_ERROR(
        tsuba::ErrorCode::NotFound,
        "failed: could not find manifest file in {}", name);
  }
  return name.Join(found_manifest);
}

}  // namespace

katana::Result<tsuba::RDGManifest>
tsuba::FindManifest(const std::string& rdg_name) {
  katana::Uri uri = KATANA_CHECKED(katana::Uri::Make(rdg_name));

  if (RDGManifest::IsManifestUri(uri)) {
    RDGManifest manifest = KATANA_CHECKED(tsuba::RDGManifest::Make(uri));
    return manifest;
  }

  auto latest_uri = FindAnyManifestForLatestVersion(uri);
  if (!latest_uri) {
    return KATANA_ERROR(
        ErrorCode::InvalidArgument, "failed to find latest RDGManifest at {}",
        uri.string());
  }

  RDGManifest manifest =
      KATANA_CHECKED(tsuba::RDGManifest::Make(latest_uri.value()));
  return manifest;
}

katana::Result<tsuba::RDGHandle>
tsuba::Open(RDGManifest rdg_manifest, uint32_t flags) {
  if (!OpenFlagsValid(flags)) {
    return KATANA_ERROR(
        ErrorCode::InvalidArgument, "invalid value for flags ({:#x})", flags);
  }

  return RDGHandle{.impl_ = new RDGHandleImpl(flags, std::move(rdg_manifest))};
}

katana::Result<void>
tsuba::Close(RDGHandle handle) {
  delete handle.impl_;
  return katana::ResultSuccess();
}

katana::Result<void>
tsuba::Create(const std::string& name) {
  katana::Uri uri = KATANA_CHECKED(katana::Uri::Make(name));

  KATANA_LOG_DEBUG_ASSERT(!RDGManifest::IsManifestUri(uri));
  // the default construction is the empty RDG
  tsuba::RDGManifest manifest{};

  katana::CommBackend* comm = Comm();
  if (comm->Rank == 0) {
    std::string s = manifest.ToJsonString();
    if (auto res = tsuba::FileStore(
            tsuba::RDGManifest::FileName(
                uri, tsuba::kDefaultRDGViewType, manifest.version())
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
  *type = KATANA_CHECKED(tsuba::RDGManifest::ParseViewNameFromName(filename));
  *args = KATANA_CHECKED(tsuba::RDGManifest::ParseViewArgsFromName(filename));
  *version = KATANA_CHECKED(tsuba::RDGManifest::ParseVersionFromName(filename));
  return katana::ResultSuccess();
}

}  // namespace

katana::Result<std::pair<uint64_t, std::vector<tsuba::RDGView>>>
tsuba::ListViewsOfVersion(
    const std::string& rdg_dir, std::optional<uint64_t> version) {
  auto rdg_uri = KATANA_CHECKED(katana::Uri::Make(rdg_dir));
  std::vector<std::string> files = KATANA_CHECKED(FileList(rdg_uri.string()));

  std::vector<tsuba::RDGView> views_found;
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

    katana::Uri manifest_path = rdg_uri.Join(file);

    const RDGManifest& manifest =
        KATANA_CHECKED(RDGManifest::Make(manifest_path));

    if (manifest.num_hosts() == 0) {
      // empty sentinal; not a valid view
      continue;
    }

    views_found.emplace_back(tsuba::RDGView{
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
          tsuba::ErrorCode::NotFound, "no views found for requested version");
    } else {
      return KATANA_ERROR(
          tsuba::ErrorCode::NotFound, "no manifest files found in directory");
    }
  }

  return std::make_pair(latest_version, views_found);
}

katana::Result<std::pair<uint64_t, std::vector<tsuba::RDGView>>>
tsuba::ListAvailableViews(
    const std::string& rdg_dir, std::optional<uint64_t> version) {
  return ListViewsOfVersion(rdg_dir, version);
}

katana::Result<std::vector<std::pair<katana::Uri, katana::Uri>>>
tsuba::CreateSrcDestFromViewsForCopy(
    const std::string& src_dir, const std::string& dst_dir, uint64_t version) {
  std::vector<std::pair<katana::Uri, katana::Uri>> src_dst_files;

  // List out all the files in a given view
  auto rdg_views = KATANA_CHECKED(tsuba::ListAvailableViews(src_dir, version));
  for (const auto& rdg_view : rdg_views.second) {
    auto rdg_view_uri = KATANA_CHECKED(katana::Uri::Make(rdg_view.view_path));
    auto rdg_manifest_res = tsuba::RDGManifest::Make(rdg_view_uri);
    if (!rdg_manifest_res) {
      KATANA_LOG_WARN(
          "not a valid manifest file: {}, {}", rdg_view_uri.string(),
          rdg_manifest_res.error());
      continue;
    }

    auto rdg_manifest = std::move(rdg_manifest_res.value());

    auto fnames = KATANA_CHECKED(rdg_manifest.FileNames());
    for (auto fname : fnames) {
      auto src_file_path = katana::Uri::JoinPath(src_dir, fname);
      auto src_file_uri = KATANA_CHECKED(katana::Uri::Make(src_file_path));

      // Skip manifests for now, will be handled at the end
      if (tsuba::RDGManifest::IsManifestUri(src_file_uri)) {
        continue;
      }

      // Check to see if we have a partition file
      // If we have a partition file, the dst path should be based on PartitionFileName using rdg manifest info
      // We're batching this now because we want to rely on having the RDG manifest file in case things
      // change in the future.
      katana::Uri dst_file_uri;
      if (tsuba::RDGPartHeader::IsPartitionFileUri(src_file_uri)) {
        auto host_id =
            KATANA_CHECKED(tsuba::RDGPartHeader::ParseHostFromPartitionFile(
                src_file_uri.BaseName()));
        auto dst_dir_uri = KATANA_CHECKED(katana::Uri::Make(dst_dir));
        dst_file_uri = tsuba::RDGManifest::PartitionFileName(
            rdg_manifest.view_specifier(), dst_dir_uri, host_id,
            1 /* version */);
      } else {
        auto dst_file_path = katana::Uri::JoinPath(dst_dir, fname);
        dst_file_uri = KATANA_CHECKED(katana::Uri::Make(dst_file_path));
      }
      src_dst_files.push_back(std::make_pair(src_file_uri, dst_file_uri));
    }

    // We add the manifest file to the vector
    auto rdg_manifest_uri = rdg_manifest.FileName();

    // Reset the version
    rdg_manifest.set_version(1);
    rdg_manifest.set_prev_version(1);

    auto dst_rdg_manifest_path =
        katana::Uri::JoinPath(dst_dir, rdg_manifest.FileName().BaseName());
    auto dst_rdg_manifest_uri =
        KATANA_CHECKED(katana::Uri::Make(dst_rdg_manifest_path));
    src_dst_files.push_back(
        std::make_pair(rdg_manifest_uri, dst_rdg_manifest_uri));
  }
  return src_dst_files;
}

katana::Result<void>
tsuba::CopyRDG(std::vector<std::pair<katana::Uri, katana::Uri>> src_dst_files) {
  // TODO(vkarthik): add do_all loop
  std::vector<uint64_t> manifest_uri_idxs;
  for (uint64_t i = 0; i < src_dst_files.size(); i++) {
    auto [src_file_uri, dst_file_uri] = src_dst_files[i];
    // We save the names of all the manifest files and we write them out at the end.
    if (tsuba::RDGManifest::IsManifestUri(src_file_uri)) {
      manifest_uri_idxs.push_back(i);
      continue;
    }
    tsuba::FileView fv;
    KATANA_CHECKED(fv.Bind(src_file_uri.string(), true));
    KATANA_CHECKED(
        tsuba::FileStore(dst_file_uri.string(), fv.ptr<char>(), fv.size()));
  }

  // Process all the manifest files, write them out.
  // We want to write this last so that we know whether a write fully finished or not.
  for (auto idx : manifest_uri_idxs) {
    auto [src_file_uri, dst_file_uri] = src_dst_files[idx];
    auto rdg_manifest = KATANA_CHECKED(tsuba::RDGManifest::Make(src_file_uri));
    // These are hard-coded for now. Will what we copy always be version 1?
    // Should we clear the lineage as well?
    // Reset the version
    rdg_manifest.set_version(1);
    rdg_manifest.set_prev_version(1);

    auto rdg_manifest_json = rdg_manifest.ToJsonString();
    KATANA_CHECKED(tsuba::FileStore(
        dst_file_uri.string(),
        reinterpret_cast<const uint8_t*>(rdg_manifest_json.data()),
        rdg_manifest_json.size()));
  }
  return katana::ResultSuccess();
}

/// Create a file name for the default CSR topology
katana::Uri
tsuba::MakeTopologyFileName(tsuba::RDGHandle handle) {
  return GetRDGDir(handle).RandFile("topology");
}

katana::Uri
tsuba::MakeNodeEntityTypeIDArrayFileName(tsuba::RDGHandle handle) {
  return GetRDGDir(handle).RandFile("node_entity_type_id_array");
}

katana::Uri
tsuba::MakeEdgeEntityTypeIDArrayFileName(tsuba::RDGHandle handle) {
  return GetRDGDir(handle).RandFile("edge_entity_type_id_array");
}

katana::Uri
tsuba::GetRDGDir(tsuba::RDGHandle handle) {
  return handle.impl_->rdg_manifest().dir();
}

katana::Result<void>
tsuba::Init(katana::CommBackend* comm) {
  katana::InitSignalHandlers();
  return GlobalState::Init(comm);
}

katana::Result<void>
tsuba::Init() {
  return Init(&default_comm_backend);
}

katana::Result<void>
tsuba::Fini() {
  auto r = GlobalState::Fini();
  return r;
}
