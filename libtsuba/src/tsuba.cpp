#include "tsuba/tsuba.h"

#include "GlobalState.h"
#include "RDGHandleImpl.h"
#include "katana/CommBackend.h"
#include "katana/Env.h"
#include "katana/Plugin.h"
#include "katana/Signals.h"
#include "tsuba/Errors.h"
#include "tsuba/file.h"
#include "tsuba/FileView.h"

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

// From CSVImport.cpp
katana::Result<uint64_t>
Weigh(const std::string& file) {
  tsuba::StatBuf buf;
  auto res = tsuba::FileStat(file, &buf);
  if (!res) {
    return res.error().WithContext(
        "cannot stat the file {}; with error {}.", file, res.error());
  }
  return buf.size;
}

// From CSVImport.cpp
katana::Result<std::vector<uint64_t>>
WeighFiles(const std::vector<std::string>& ops) {
  auto& net = katana::getSystemNetworkInterface();
  auto [begin_file_idx, end_file_idx] =
      katana::block_range(uint64_t{0}, uint64_t{ops.size()}, net.ID, net.Num);

  std::vector<uint64_t> result(
      ops.size(), std::numeric_limits<uint64_t>::max());

  katana::PerThreadStorage<std::optional<katana::CopyableErrorInfo>> error;
  katana::do_all(
      katana::iterate(begin_file_idx, end_file_idx),
      [&](uint64_t i) {
        if (error.getLocal()->has_value()) {
          return;
        }
        auto weigh_res = Weigh(ops[i].op.file);
        if (!weigh_res) {
          *error.getLocal() = katana::CopyableErrorInfo(
              weigh_res.error().WithContext("reading {}", ops[i].op.file));
          return;
        }
        result[i] = weigh_res.value();
      },
      katana::steal());

  for (auto local_error : error) {
    if (local_error) {
      return KATANA_ERROR(local_error->error_code(), "{}", *local_error);
    }
  }

  for (katana::HostID id = katana::HostID{0}; id.value() < net.Num; ++id) {
    if (id.value() == net.ID) {
      continue;
    }
    katana::SendBuffer buf;
    katana::gSerialize(
        buf, begin_file_idx,
        std::vector<uint64_t>(
            result.begin() + begin_file_idx, result.begin() + end_file_idx));
    net.Send(id.value(), std::move(buf));
  }

  for (katana::HostID id = katana::HostID{0}; id.value() < net.Num; ++id) {
    if (id.value() == net.ID) {
      continue;
    }
    uint64_t other_begin;
    std::vector<uint64_t> other_result;

    auto recv_result = net.Recv();
    katana::gDeserialize(recv_result.second, other_begin, other_result);

    std::copy(
        other_result.begin(), other_result.end(), result.begin() + other_begin);
  }
  net.EndCommunicationPhase();

  return result;
}

// Adapted from CSVImport.cpp
std::pair<std::pair<uint64_t, uint64_t>, std::pair<uint64_t, uint64_t>>
SliceWeightedVectorForHost(
    std::vector<uint64_t> weights, katana::HostID host_id,
    katana::HostID num_hosts) {
  if (weights.empty()) {
    return std::make_pair(std::make_pair(0, 0), std::make_pair(0, 0));
  }
  if (weights.size() <= num_hosts) {
    return std::make_pair(
        std::make_pair(std::min(size_t{host_id.value()}, weights.size()), 0),
        std::make_pair(
            std::min(size_t{host_id.value() + 1}, weights.size()), 0));
  }
  katana::ParallelSTL::partial_sum(
      weights.begin(), weights.end(), weights.begin());

  uint64_t host_weight =
      (weights.back() + num_hosts.value() - 1) / num_hosts.value();

  std::vector<uint64_t> per_host_end_idx(num_hosts.value());
  for (katana::HostID i = katana::HostID{0}; i < num_hosts; ++i) {
    auto it = std::upper_bound(
        weights.begin(), weights.end(), (i.value() + 1) * host_weight);
    per_host_end_idx[i.value()] = std::distance(weights.begin(), it);
  }

  KATANA_LOG_VASSERT(
      per_host_end_idx.back() == weights.size(), "{} vs {}",
      per_host_end_idx.back(), weights.size());

  auto files_assigned_to_neighbors = [&per_host_end_idx](uint64_t host_id) {
    uint64_t last = per_host_end_idx.size() - 1;
    uint64_t on_the_left =
        (host_id == 0 ? 0
                      : per_host_end_idx[host_id - 1] -
                            (host_id <= 1 ? 0 : per_host_end_idx[host_id - 2]));
    uint64_t on_the_right =
        (host_id == last
             ? 0
             : per_host_end_idx[host_id + 1] - per_host_end_idx[host_id]);

    return std::make_pair(on_the_left, on_the_right);
  };

  // deterministically move things around to make sure every host has at least
  // one value (required for now)
  uint32_t hosts_with_no_files;
  do {
    hosts_with_no_files = num_hosts.value();
    for (uint64_t i = 0; i < per_host_end_idx.size(); ++i) {
      uint64_t start = i == 0 ? 0 : per_host_end_idx[i - 1];
      uint64_t end = per_host_end_idx[i];
      if (start != end) {
        --hosts_with_no_files;
        continue;
      }
      auto [on_left, on_right] = files_assigned_to_neighbors(i);
      if (on_left > 0 && on_left >= on_right) {
        --per_host_end_idx[i - 1];
        if (on_left <= 1) {
          hosts_with_no_files += 1;
        }
      } else if (on_right > 0) {
        ++per_host_end_idx[i];
      }
      --hosts_with_no_files;
    }
  } while (hosts_with_no_files > 0);

  uint64_t start = (host_id == 0 ? 0 : per_host_end_idx[host_id.value() - 1]);
  uint64_t end = per_host_end_idx[host_id.value()];
  return std::make_pair(std::make_pair(start, 0), std::make_pair(end, 0));
}


katana::Result<std::vector<std::pair<std::string, std::string>>>
tsuba::ListAllFilesFromView(const std::string& src_dir, const std::string& dst_sir, uint64_t version) {
  std::vector<std::pair<std::string, std::string>>> src_dest_pairs;

  return src_dest_pairs;
}


katana::Result<void>
tsuba::CopyRDG(
    const std::string& src_dir, const std::string& dst_dir, uint64_t version) {
  // List out all the files in a given view
  auto rdg_views_res = tsuba::ListAvailableViewsFromVersion(
      src_dir, version);
  for (const auto& rdg_view : rdg_views_res.value()) {
    KATANA_LOG_WARN("view_path: {}", rdg_view.view_path);
    auto uri = KATANA_CHECKED(katana::Uri::Make(src_dir));

    auto rdg_manifest_res = tsuba::RDGManifest::Make(
        uri, rdg_view.view_type, version);
    if (!rdg_manifest_res) {
      continue;
    }
    auto fnames = KATANA_CHECKED(rdg_manifest_res.value().FileNames());

    std::string manifest_file{};
    // Write out the data first
    for (const auto fname : fnames) {
      KATANA_LOG_WARN("view_fname: {}", fname);
      auto src_joined_path = katana::Uri::JoinPath(src_dir, fname);
      KATANA_LOG_WARN("src_joined_path: {}", src_joined_path);
      auto src = KATANA_CHECKED(katana::Uri::Make(src_joined_path));

      // Let's skip copying this over for now. Let's just get the pure name of the file
      if (tsuba::RDGManifest::IsManifestUri(src)) {
        KATANA_LOG_WARN("found a manifest file");
        manifest_file = fname;
        continue;
      }
      auto dst_joined_path = katana::Uri::JoinPath(dst_dir, fname);
      KATANA_LOG_WARN("dst_joined_path: {}", dst_joined_path);
      auto dst = KATANA_CHECKED(katana::Uri::Make(dst_joined_path));

      // Now copy it over
      tsuba::FileView fv;
      KATANA_CHECKED(fv.Bind(src.path(), false));
      KATANA_CHECKED(tsuba::FileStore(dst.path(), fv.ptr<char>(), fv.size()));
    }
    // Write out the manifest file as a final commit!
    // TODO: make sure to change the version?
    tsuba::FileView fv;
    auto dst_manifest_path = katana::Uri::JoinPath(dst_dir, manifest_file);
    KATANA_LOG_WARN("dst_manifest_path: {}", dst_manifest_path);
    KATANA_CHECKED(fv.Bind(rdg_view.view_path, false));
    KATANA_CHECKED(tsuba::FileStore(dst_manifest_path, fv.ptr<char>(), fv.size()));
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
