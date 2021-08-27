#include "tsuba/tsuba.h"

#include "GlobalState.h"
#include "RDGHandleImpl.h"
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

  if (auto res = list_fut.get(); !res) {
    return res.error();
  }
  return files;
}

katana::Result<katana::Uri>
FindLatestManifestFile(const katana::Uri& name) {
  KATANA_LOG_DEBUG_ASSERT(!tsuba::RDGManifest::IsManifestUri(name));
  auto list_res = FileList(name.string());
  if (!list_res) {
    return list_res.error();
  }

  uint64_t version = 0;
  std::string found_manifest;
  for (const std::string& file : list_res.value()) {
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

katana::Result<tsuba::RDGHandle>
tsuba::Open(const std::string& rdg_name, uint32_t flags) {
  if (!OpenFlagsValid(flags)) {
    return KATANA_ERROR(
        ErrorCode::InvalidArgument, "invalid value for flags ({:#x})", flags);
  }

  auto uri_res = katana::Uri::Make(rdg_name);
  if (!uri_res) {
    return uri_res.error();
  }
  katana::Uri uri = std::move(uri_res.value());

  if (RDGManifest::IsManifestUri(uri)) {
    auto manifest_res = tsuba::RDGManifest::Make(uri);
    if (!manifest_res) {
      return manifest_res.error();
    }

    return RDGHandle{
        .impl_ = new RDGHandleImpl(flags, std::move(manifest_res.value()))};
  }

  auto latest_uri = FindLatestManifestFile(uri);
  if (!latest_uri) {
    return KATANA_ERROR(
        ErrorCode::InvalidArgument, "failed to find latest RDGManifest at {}",
        uri.string());
  }

  auto manifest_res = tsuba::RDGManifest::Make(latest_uri.value());
  if (!manifest_res) {
    return manifest_res.error();
  }

  return RDGHandle{
      .impl_ = new RDGHandleImpl(flags, std::move(manifest_res.value()))};
}

katana::Result<void>
tsuba::Close(RDGHandle handle) {
  delete handle.impl_;
  return katana::ResultSuccess();
}

katana::Result<void>
tsuba::Create(const std::string& name) {
  auto uri_res = katana::Uri::Make(name);
  if (!uri_res) {
    return uri_res.error();
  }
  katana::Uri uri = std::move(uri_res.value());

  KATANA_LOG_DEBUG_ASSERT(!RDGManifest::IsManifestUri(uri));
  // the default construction is the empty RDG
  tsuba::RDGManifest manifest{};

  katana::CommBackend* comm = Comm();
  if (comm->ID == 0) {
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
tsuba::ListAvailableViews(
    const std::string& rdg_dir, std::optional<uint64_t> version) {
  auto rdg_uri = KATANA_CHECKED(katana::Uri::Make(rdg_dir));
  std::vector<std::string> files = KATANA_CHECKED(FileList(rdg_uri.string()));

  std::vector<tsuba::RDGView> views_found;
  uint64_t latest_version = 0;
  bool found_graph = false;
  for (const std::string& file : files) {
    std::string view_type;
    std::vector<std::string> view_args;
    uint64_t view_version{};

    if (!ParseManifestName(file, &view_type, &view_args, &view_version)) {
      continue;
    }

    if (version && view_version != version.value()) {
      continue;
    }

    katana::Uri manifest_path = rdg_uri.Join(file);

    auto manifest_res = RDGManifest::Make(manifest_path);
    if (!manifest_res) {
      continue;
    }
    const RDGManifest& manifest = manifest_res.value();

    if (view_version > latest_version) {
      // we only keep views from the latest
      latest_version = view_version;
      views_found.clear();
    }

    found_graph = true;

    if (manifest.num_hosts() == 0) {
      // emtpy sentinal; not a valid view
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

  if (!found_graph) {
    return KATANA_ERROR(
        tsuba::ErrorCode::NotFound, "no views found for that version");
  }

  return std::make_pair(latest_version, views_found);
}

// vkarthik: We could combine this with the function above. I just didn't want to clutter it.
katana::Result<std::vector<tsuba::RDGView>>
tsuba::ListAvailableViewsFromVersion(
    const std::string& rdg_dir, uint64_t version) {
  std::vector<tsuba::RDGView> views_found;
  KATANA_LOG_DEBUG("ListAvailableViewsFromVersion");
  auto list_res = FileList(rdg_dir);
  if (!list_res) {
    KATANA_LOG_DEBUG("failed to list files in {}", rdg_dir);
    return list_res.error();
  }

  for (const std::string& file : list_res.value()) {
    auto view_type_res = tsuba::RDGManifest::ParseViewNameFromName(file);
    auto view_args_res = tsuba::RDGManifest::ParseViewArgsFromName(file);
    auto view_version_res = tsuba::RDGManifest::ParseVersionFromName(file);

    if (!view_type_res || !view_args_res || !view_version_res ||
        view_version_res.value() != version) {
      continue;
    }

    std::string rdg_path = fmt::format("{}/{}", rdg_dir, file);

    auto rdg_uri = katana::Uri::Make(rdg_path);
    if (!rdg_uri)
      continue;

    auto rdg_res = RDGManifest::Make(rdg_uri.value());
    if (!rdg_res)
      continue;

    RDGManifest manifest = rdg_res.value();

    std::vector<std::string> args_vector = std::move(view_args_res.value());
    views_found.push_back(tsuba::RDGView{
        .view_version = view_version_res.value(),
        .view_type = view_type_res.value(),
        .view_args = fmt::format("{}", fmt::join(args_vector, "-")),
        .view_path = fmt::format("{}/{}", rdg_dir, file),
        .num_partitions = manifest.num_hosts(),
        .policy_id = manifest.policy_id(),
        .transpose = manifest.transpose(),
    });
  }

  return views_found;
}

katana::Result<std::vector<std::pair<katana::Uri, katana::Uri>>>
tsuba::CreateSrcDestFromViewsForCopy(
    const std::string& src_dir, const std::string& dst_dir, uint64_t version) {
  std::vector<std::pair<katana::Uri, katana::Uri>> src_dst_pairs;

  // List out all the files in a given view
  auto rdg_views =
      KATANA_CHECKED(tsuba::ListAvailableViewsFromVersion(src_dir, version));
  for (const auto& rdg_view : rdg_views) {
    KATANA_LOG_WARN("view_path: {}", rdg_view.view_path);
    auto uri = KATANA_CHECKED(katana::Uri::Make(src_dir));

    auto rdg_manifest_res =
        tsuba::RDGManifest::Make(uri, rdg_view.view_type, version);
    if (!rdg_manifest_res) {
      continue;
    }

    auto fnames = KATANA_CHECKED(rdg_manifest_res.value().FileNames());
    for (auto fname : fnames) {
      auto src_file_path = katana::Uri::JoinPath(src_dir, fname);
      auto src_file_uri = KATANA_CHECKED(katana::Uri::Make(src_file_path));

      // If we are a manifest, we need to change our version to 1.
      if (tsuba::RDGManifest::IsManifestUri(src_file_uri)) {
        continue;
      }

      // Check to see if we have a partition file
      // If we have a partition file, the dst path should be based on PartitionFileName using rdg manifest info
      // We're batching this now because we want to rely on having the RDG manifest file in case things
      // change in the future.
      katana::Uri dst_file_uri;
      if (tsuba::RDGManifest::IsPartitionFileUri(src_file_uri)) {
        KATANA_LOG_WARN("src_file_uri partition: {}", src_file_uri);
        auto host_id =
            KATANA_CHECKED(tsuba::RDGManifest::ParseHostFromPartitionFile(
                src_file_uri.BaseName()));
        auto dst_dir_uri = KATANA_CHECKED(katana::Uri::Make(dst_dir));
        dst_file_uri = tsuba::RDGManifest::PartitionFileName(
            rdg_manifest_res.value().view_type(), dst_dir_uri, host_id, 1);
        KATANA_LOG_WARN("dst_file_uri partition: {}", dst_file_uri);
      } else {
        auto dst_file_path = katana::Uri::JoinPath(dst_dir, fname);
        dst_file_uri = KATANA_CHECKED(katana::Uri::Make(dst_file_path));
        KATANA_LOG_WARN("src_file_uri: {}", src_file_uri);
        KATANA_LOG_WARN("dst_file_uri: {}", dst_file_uri);
      }

      src_dst_pairs.push_back(std::make_pair(src_file_uri, dst_file_uri));
    }

    // We add the manifest file to the vector
    // Set the version to be 1
    auto rdg_manifest_uri = rdg_manifest_res.value().FileName();
    rdg_manifest_res.value().ResetVersion();
    auto dst_rdg_manifest_path = katana::Uri::JoinPath(
        dst_dir, rdg_manifest_res.value().FileName().BaseName());
    auto dst_rdg_manifest_uri =
        KATANA_CHECKED(katana::Uri::Make(dst_rdg_manifest_path));
    KATANA_LOG_WARN("rdg_manifest_uri: {}", rdg_manifest_uri);
    KATANA_LOG_WARN("dst_rdg_manifest_uri: {}", dst_rdg_manifest_uri);
    src_dst_pairs.push_back(
        std::make_pair(rdg_manifest_uri, dst_rdg_manifest_uri));
  }
  return src_dst_pairs;
}

katana::Result<void>
tsuba::CopyRDG(std::vector<std::pair<katana::Uri, katana::Uri>> src_dst_pairs) {
  // TODO: make sure that manifests are written at the end!
  // TODO: add do_all loop
  std::vector<uint64_t> manifest_uri_idxs;
  for (uint64_t i = 0; i < src_dst_pairs.size(); i++) {
    auto [src_file_uri, dst_file_uri] = src_dst_pairs[i];
    // We save the names of all the manifest files and we write them out at the end.
    if (tsuba::RDGManifest::IsManifestUri(src_file_uri)) {
      manifest_uri_idxs.push_back(i);
      continue;
    }
    tsuba::FileView fv;
    KATANA_CHECKED(fv.Bind(src_file_uri.path(), false));
    KATANA_CHECKED(
        tsuba::FileStore(dst_file_uri.path(), fv.ptr<char>(), fv.size()));
  }

  // Process all the manifest files, write them out.
  // We want to write this last so that we know whether a write fully finished or not.
  for (auto idx : manifest_uri_idxs) {
    auto [src_file_uri, dst_file_uri] = src_dst_pairs[idx];
    auto rdg_manifest = KATANA_CHECKED(tsuba::RDGManifest::Make(src_file_uri));
    // These are hard-coded for now. Will what we copy always be version 1?
    // Should we clear the lineage as well?
    rdg_manifest.ResetVersion();
    auto rdg_manifest_json = rdg_manifest.ToJsonString();
    KATANA_CHECKED(tsuba::FileStore(
        dst_file_uri.path(),
        reinterpret_cast<const uint8_t*>(rdg_manifest_json.data()),
        rdg_manifest_json.size()));
  }
  return katana::ResultSuccess();
}

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
