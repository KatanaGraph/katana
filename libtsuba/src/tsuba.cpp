#include "tsuba/tsuba.h"

#include "GlobalState.h"
#include "RDGHandleImpl.h"
#include "katana/CommBackend.h"
#include "katana/Env.h"
#include "katana/RDGVersion.h"
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

katana::Result<tsuba::RDGStat>
tsuba::Stat(const std::string& rdg_name) {
  auto uri_res = katana::Uri::Make(rdg_name);
  if (!uri_res) {
    return uri_res.error();
  }
  katana::Uri uri = std::move(uri_res.value());

  if (!RDGManifest::IsManifestUri(uri)) {
    if (auto res = FindLatestManifestFile(uri); !res) {
      return res.error().WithContext("failed to find an RDG manifest");
    } else {
      auto rdg_res = RDGManifest::Make(res.value());
      RDGManifest manifest = rdg_res.value();
      return RDGStat{
          .num_partitions = manifest.num_hosts(),
          .policy_id = manifest.policy_id(),
          .transpose = manifest.transpose(),
      };
    }
  }

  auto rdg_res = RDGManifest::Make(uri);
  if (!rdg_res) {
    if (rdg_res.error() == katana::ErrorCode::JSONParseFailed) {
      return RDGStat{
          .num_partitions = 1,
          .policy_id = 0,
          .transpose = false,
      };
    }
    return rdg_res.error();
  }

  RDGManifest manifest = rdg_res.value();
  return RDGStat{
      .num_partitions = manifest.num_hosts(),
      .policy_id = manifest.policy_id(),
      .transpose = manifest.transpose(),
  };
}

katana::Result<std::vector<tsuba::RDGView>>
tsuba::ListAvailableViewsForBranch(const std::string& rdg_dir, 
    bool find_max_version,
    RDGVersion version, bool* is_intermiate_version) {

  std::vector<tsuba::RDGView> views_found;
  KATANA_LOG_DEBUG("ListAvailableViews for a branch");

  // For a path to the directory targeted by version
  std::string target_dir = rdg_dir;
  /*std::vector<std::string> */
  auto branches = version.GetBranchIDs();
  for (auto & branch : branches) {
    if (branch !="") {
      target_dir += "/";
      target_dir += branch;
    }
  }

  // TODO(wkyu): filter out the directories from the FileList.
  auto list_res = FileList(target_dir);
  if (!list_res) {
    KATANA_LOG_DEBUG("failed to list files in {}", target_dir);
    return list_res.error();
  }

  std::vector<uint64_t> long_ver_nums = version.GetBranchNumbers();
  // TODO(wkyu): assert the correctness of the first few branch numbers. 
  uint64_t target_version = long_ver_nums[long_ver_nums.size()-1];
  uint64_t max_version = target_version;

  //TODO (yasser): add an optional parameter to function which if specified is used to set
  //'target_version' value and will set find_max_version to false
  for (const std::string& file : list_res.value()) {
    auto view_type_res = tsuba::RDGManifest::ParseViewNameFromName(file);
    auto view_args_res = tsuba::RDGManifest::ParseViewArgsFromName(file);
    auto view_version_res = tsuba::RDGManifest::ParseVersionFromName(file);

    // TODO(wkyu): filter out the directories from the FileList.
    if (!view_type_res || !view_args_res || !view_version_res ||
        view_version_res.value() < target_version) {
      continue;
    }

    // Take only the targeted version
    if (!find_max_version && view_version_res.value() > target_version) {
      continue;
    }

    if (view_version_res.value() > max_version) {
      max_version = view_version_res.value();
    }

    // If RDGManifest version is greater than our current minimum then bump up minimum and
    // discard previously found views
    if (find_max_version && (view_version_res.value() > target_version)) {
      target_version = view_version_res.value();
      views_found.clear();
    }

    std::string rdg_path = fmt::format("{}/{}", target_dir, file);

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
        .view_path = fmt::format("{}/{}", target_dir, file),
        .num_partitions = manifest.num_hosts(),
        .policy_id = manifest.policy_id(),
        .transpose = manifest.transpose(),
    });
  }

  // Views cannot be an intermediate if find_max_version is true.
  *is_intermiate_version = max_version > target_version;

  return views_found;
}


katana::Result<std::vector<tsuba::RDGView>>
tsuba::ListAvailableViews(const std::string& rdg_dir) {
  std::vector<tsuba::RDGView> views_found;
  KATANA_LOG_DEBUG("ListAvailableViews");
  auto list_res = FileList(rdg_dir);
  if (!list_res) {
    KATANA_LOG_DEBUG("failed to list files in {}", rdg_dir);
    return list_res.error();
  }

  bool find_max_version = true;
  uint64_t min_version = 1;

  //TODO (yasser): add an optional parameter to function which if specified is used to set
  //'min_version' value and will set find_max_version to false

  for (const std::string& file : list_res.value()) {
    auto view_type_res = tsuba::RDGManifest::ParseViewNameFromName(file);
    auto view_args_res = tsuba::RDGManifest::ParseViewArgsFromName(file);
    auto view_version_res = tsuba::RDGManifest::ParseVersionFromName(file);

    if (!view_type_res || !view_args_res || !view_version_res ||
        view_version_res.value() < min_version) {
      continue;
    }

    // Take only the targeted version
    if (!find_max_version && view_version_res.value() > min_version) {
      continue;
    }

    // If RDGManifest version is greater than our current minimum then bump up minimum and
    // discard previously found views
    if (find_max_version && (view_version_res.value() > min_version)) {
      min_version = view_version_res.value();
      views_found.clear();
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

katana::Uri
tsuba::MakeTopologyFileName(tsuba::RDGHandle handle) {
  return GetRDGDir(handle).RandFile("topology");
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
