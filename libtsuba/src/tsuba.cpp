#include "tsuba/tsuba.h"

#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include <iostream>

#include "GlobalState.h"
#include "RDGHandleImpl.h"
#include "katana/CommBackend.h"
#include "katana/Env.h"
#include "katana/Plugin.h"
#include "katana/RDGVersion.h"
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
FindManifestFileForVersion(
    const katana::Uri& name, katana::RDGVersion version) {
  KATANA_LOG_DEBUG_ASSERT(!tsuba::RDGManifest::IsManifestUri(name));
  auto list_res = FileList(name.string());
  if (!list_res) {
    return list_res.error();
  }

  katana::RDGVersion target = version;
  uint64_t specific = version.LeafNumber();
  if (specific >= katana::kRDGVersionMaxID) {
    specific = 0;
    target.SetLeafNumber(0);
  }

  std::string found_manifest;
  katana::RDGVersion latest;

  for (const std::string& file : list_res.value()) {
    if (auto res = tsuba::RDGManifest::ParseVersionFromName(file); res) {
      auto candidate = res.value();
      if (specific && candidate == target) {
        found_manifest = file;
        break;
      }

      if (!specific) {
        if ((candidate == latest && found_manifest.empty()) ||
            (target.ShareBranch(candidate) && candidate > latest)) {
          latest = candidate;
          found_manifest = file;
        }
      }
    }
  }

  if (found_manifest.empty()) {
    return KATANA_ERROR(
        tsuba::ErrorCode::NotFound,
        "failed: could not find manifest file in {} for target {}; ", name,
        target.ToString());
  }

  return name.Join(found_manifest);
}

katana::Result<katana::Uri>
FindLatestManifestFile(const katana::Uri& name) {
  KATANA_LOG_DEBUG_ASSERT(!tsuba::RDGManifest::IsManifestUri(name));
  auto list_res = FileList(name.string());
  if (!list_res) {
    return list_res.error();
  }

  katana::RDGVersion version;
  std::string found_manifest;

  for (const std::string& file : list_res.value()) {
    if (auto res = tsuba::RDGManifest::ParseVersionFromName(file); res) {
      auto new_version = res.value();
      // only take a newer version from the same branch
      if (new_version.ShareBranch(version) && new_version > version) {
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
  katana::RDGVersion target = katana::RDGVersion(0);
  return tsuba::Open(rdg_name, target, flags);
}

katana::Result<tsuba::RDGHandle>
tsuba::Open(
    const std::string& rdg_name, katana::RDGVersion version, uint32_t flags) {
  // This also requires a meaningful LeafNumber
  // Searches one or all manifest files in a branch
  uint64_t target = version.LeafNumber();

  if (target >= katana::kRDGVersionMaxID) {
    target = 0;
  }

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

  // Add the targeted branch before search
  auto long_uri_res = katana::Uri::Make(rdg_name);
  if (!long_uri_res) {
    return long_uri_res.error();
  }
  katana::Uri long_uri = std::move(long_uri_res.value());

  auto latest_uri = FindManifestFileForVersion(long_uri, version);
  if (!latest_uri) {
    return KATANA_ERROR(
        ErrorCode::InvalidArgument,
        "failed to find latest RDGManifest for {} at {}; ", version.ToString(),
        long_uri.string());
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
tsuba::Create(const std::string& name, katana::RDGVersion version) {
  auto uri_res = katana::Uri::Make(name);
  if (!uri_res) {
    return uri_res.error();
  }
  katana::Uri uri = std::move(uri_res.value());

  KATANA_LOG_DEBUG_ASSERT(!RDGManifest::IsManifestUri(uri));

  tsuba::RDGManifest manifest{};
  if (!version.IsNull()) {
    // start a new branch with v0
    version.SetLeafNumber(0);
    manifest.set_version(version);
  }

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
tsuba::ListAvailableViewsForVersion(
    const std::string& rdg_dir, katana::RDGVersion version,
    katana::RDGVersion* max_version) {
  std::vector<tsuba::RDGView> views_found;
  katana::RDGVersion target = version;
  uint64_t specific = version.LeafNumber();
  if (specific >= katana::kRDGVersionMaxID) {
    specific = 0;
    target.SetLeafNumber(0);
  }

  auto list_res = FileList(rdg_dir);
  if (!list_res) {
    return list_res.error().WithContext("failed to list files in {}", rdg_dir);
  }

  katana::RDGVersion current_max = katana::RDGVersion(0);

  // Slight modification from Yasser's code to find only targeted version
  for (const std::string& file : list_res.value()) {
    auto view_type_res = tsuba::RDGManifest::ParseViewNameFromName(file);
    auto view_args_res = tsuba::RDGManifest::ParseViewArgsFromName(file);
    auto view_version_res = tsuba::RDGManifest::ParseVersionFromName(file);

    // filter out all versions not in the same branch
    if (!view_type_res || !view_args_res || !view_version_res ||
        !view_version_res.value().LeafNumber() ||
        !view_version_res.value().ShareBranch(target)) {
      continue;
    }

    // Update curent_max
    if (view_version_res.value() > current_max) {
      current_max = view_version_res.value();
      // Clear views_found if max is needed
      if (!specific) {
        views_found.clear();
      }
    }

    if (specific && (view_version_res.value() != target)) {
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

  // After the search, current_max is the maximum from all views
  // target_version is the version for Views found.
  *max_version = current_max;

  KATANA_LOG_DEBUG(
      "ListAvailableViewsForVersion {} max {} candidates={} ",
      target.ToString(), current_max.ToString(), views_found.size());
  return views_found;
}

katana::Result<std::vector<tsuba::RDGView>>
tsuba::ListAvailableViews(const std::string& rdg_dir) {
  std::vector<tsuba::RDGView> views_found;
  auto list_res = FileList(rdg_dir);
  if (!list_res) {
    return list_res.error();
  }

  bool find_max_version = true;
  katana::RDGVersion min_version = katana::RDGVersion(1);

  //TODO (yasser): add an optional parameter to function which if specified is used to set
  //'min_version' value and will set find_max_version to false
  for (const std::string& file : list_res.value()) {
    auto view_type_res = tsuba::RDGManifest::ParseViewNameFromName(file);
    auto view_args_res = tsuba::RDGManifest::ParseViewArgsFromName(file);
    auto view_version_res = tsuba::RDGManifest::ParseVersionFromName(file);

    // Ignore all versions not in the same branch or trunk
    if (!view_type_res || !view_args_res || !view_version_res ||
        !view_version_res.value().ShareBranch(min_version) ||
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
