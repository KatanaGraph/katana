#include "tsuba/tsuba.h"

#include "GlobalState.h"
#include "RDGHandleImpl.h"
#include "katana/Backtrace.h"
#include "katana/CommBackend.h"
#include "katana/Env.h"
#include "tsuba/Errors.h"
#include "tsuba/NameServerClient.h"
#include "tsuba/Preload.h"
#include "tsuba/file.h"

namespace {

katana::NullCommBackend default_comm_backend;
std::unique_ptr<tsuba::NameServerClient> default_ns_client;

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

bool
ContainsValidMetaFile(const std::string& dir) {
  auto list_res = FileList(dir);
  if (!list_res) {
    KATANA_LOG_DEBUG(
        "ContainsValidMetaFile dir: {}: {}", dir, list_res.error());
    return false;
  }
  for (const std::string& file : list_res.value()) {
    if (auto res = tsuba::RDGMeta::ParseVersionFromName(file); res) {
      return true;
    }
  }
  return false;
}

katana::Result<katana::Uri>
FindLatestMetaFile(const katana::Uri& name) {
  KATANA_LOG_DEBUG_ASSERT(!tsuba::RDGMeta::IsMetaUri(name));
  auto list_res = FileList(name.string());
  if (!list_res) {
    return list_res.error();
  }

  uint64_t version = 0;
  std::string found_meta;
  for (const std::string& file : list_res.value()) {
    if (auto res = tsuba::RDGMeta::ParseVersionFromName(file); res) {
      uint64_t new_version = res.value();
      if (new_version >= version) {
        version = new_version;
        found_meta = file;
      }
    }
  }
  if (found_meta.empty()) {
    KATANA_LOG_DEBUG("failed: could not find meta file in {}", name);
    return tsuba::ErrorCode::NotFound;
  }
  return name.Join(found_meta);
}

}  // namespace

katana::Result<tsuba::RDGHandle>
tsuba::Open(const std::string& rdg_name, uint32_t flags) {
  if (!OpenFlagsValid(flags)) {
    KATANA_LOG_ERROR("invalid value for flags ({:#x})", flags);
    return ErrorCode::InvalidArgument;
  }

  auto uri_res = katana::Uri::Make(rdg_name);
  if (!uri_res) {
    return uri_res.error();
  }
  katana::Uri uri = std::move(uri_res.value());

  if (RDGMeta::IsMetaUri(uri)) {
    KATANA_LOG_DEBUG(
        "failed: {} is probably a literal rdg file and not suited for open",
        uri);
    return ErrorCode::InvalidArgument;
  }

  if (!RDGMeta::IsMetaUri(uri)) {
    // try to be helpful and look for RDGs that we don't know about
    if (auto res = RegisterIfAbsent(uri.string()); !res) {
      KATANA_LOG_DEBUG("failed to auto-register: {}", res.error());
      return res.error();
    }
  }

  auto meta_res = tsuba::RDGMeta::Make(uri);
  if (!meta_res) {
    return meta_res.error();
  }

  return RDGHandle{
      .impl_ = new RDGHandleImpl(flags, std::move(meta_res.value()))};
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

  KATANA_LOG_DEBUG_ASSERT(!RDGMeta::IsMetaUri(uri));
  // the default construction is the empty RDG
  tsuba::RDGMeta meta{};

  katana::CommBackend* comm = Comm();
  if (comm->ID == 0) {
    if (ContainsValidMetaFile(name)) {
      KATANA_LOG_ERROR(
          "unable to create {}: path already contains a valid meta file", name);
      return ErrorCode::InvalidArgument;
    }
    std::string s = meta.ToJsonString();
    if (auto res = tsuba::FileStore(
            tsuba::RDGMeta::FileName(uri, meta.version()).string(),
            reinterpret_cast<const uint8_t*>(s.data()), s.size());
        !res) {
      KATANA_LOG_ERROR("failed to store RDG file");
      comm->NotifyFailure();
      return res.error();
    }
  }

  // NS handles MPI coordination
  if (auto res = tsuba::NS()->CreateIfAbsent(uri, meta); !res) {
    KATANA_LOG_ERROR("failed to create RDG name");
    return res.error();
  }

  return katana::ResultSuccess();
}

katana::Result<void>
tsuba::RegisterIfAbsent(const std::string& name) {
  auto uri_res = katana::Uri::Make(name);
  if (!uri_res) {
    return uri_res.error();
  }
  katana::Uri uri = std::move(uri_res.value());

  if (!RDGMeta::IsMetaUri(uri)) {
    auto latest_res = FindLatestMetaFile(uri);
    if (!latest_res) {
      return latest_res.error();
    }
    uri = std::move(latest_res.value());
  }

  auto meta_res = RDGMeta::Make(uri);
  if (!meta_res) {
    return meta_res.error();
  }
  RDGMeta meta = std::move(meta_res.value());

  return tsuba::NS()->CreateIfAbsent(meta.dir(), meta);
}

katana::Result<void>
tsuba::Forget(const std::string& name) {
  auto uri_res = katana::Uri::Make(name);
  if (!uri_res) {
    return uri_res.error();
  }
  katana::Uri uri = std::move(uri_res.value());

  if (RDGMeta::IsMetaUri(uri)) {
    KATANA_LOG_DEBUG("uri does not look like a graph name (ends in meta)");
    return ErrorCode::InvalidArgument;
  }

  // NS ensures only host 0 creates
  auto res = tsuba::NS()->Delete(uri);
  return res;
}

katana::Result<tsuba::RDGStat>
tsuba::Stat(const std::string& rdg_name) {
  auto uri_res = katana::Uri::Make(rdg_name);
  if (!uri_res) {
    return uri_res.error();
  }
  katana::Uri uri = std::move(uri_res.value());

  if (!RDGMeta::IsMetaUri(uri)) {
    // try to be helpful and look for RDGs that we don't know about
    if (auto res = RegisterIfAbsent(uri.string()); !res) {
      KATANA_LOG_DEBUG("failed to auto-register: {}", res.error());
      return res.error();
    }
  }

  auto rdg_res = RDGMeta::Make(uri);
  if (!rdg_res) {
    if (rdg_res.error() == katana::ErrorCode::JsonParseFailed) {
      return RDGStat{
          .num_hosts = 1,
          .policy_id = 0,
          .transpose = false,
      };
    }
    return rdg_res.error();
  }

  RDGMeta meta = rdg_res.value();
  return RDGStat{
      .num_hosts = meta.num_hosts(),
      .policy_id = meta.policy_id(),
      .transpose = meta.transpose(),
  };
}

katana::Uri
tsuba::MakeTopologyFileName(tsuba::RDGHandle handle) {
  return GetRDGDir(handle).RandFile("topology");
}

katana::Uri
tsuba::GetRDGDir(tsuba::RDGHandle handle) {
  return handle.impl_->rdg_meta().dir();
}

katana::Result<void>
tsuba::Init(katana::CommBackend* comm) {
  tsuba::Preload();
  auto client_res = GlobalState::MakeNameServerClient();
  if (!client_res) {
    return client_res.error();
  }
  default_ns_client = std::move(client_res.value());
  katana::InitBacktrace();
  return GlobalState::Init(comm, default_ns_client.get());
}

katana::Result<void>
tsuba::Init() {
  return Init(&default_comm_backend);
}

katana::Result<void>
tsuba::Fini() {
  auto r = GlobalState::Fini();
  tsuba::PreloadFini();
  return r;
}
