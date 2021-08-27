#include "tsuba/tsuba.h"

#include "GlobalState.h"
#include "RDGHandleImpl.h"
#include "katana/CommBackend.h"
#include "katana/Env.h"
#include "katana/Plugin.h"
#include "katana/Signals.h"
#include "tsuba/Errors.h"
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
