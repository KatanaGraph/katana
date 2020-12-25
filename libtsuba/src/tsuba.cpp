#include "tsuba/tsuba.h"

#include "GlobalState.h"
#include "HttpNameServerClient.h"
#include "MemoryNameServerClient.h"
#include "RDGHandleImpl.h"
#include "galois/Backtrace.h"
#include "galois/CommBackend.h"
#include "galois/Env.h"
#include "tsuba/Preload.h"

namespace {

galois::NullCommBackend default_comm_backend;
tsuba::MemoryNameServerClient default_ns_client;

galois::Result<std::vector<std::string>>
FileList(const std::string& dir) {
  std::vector<std::string> files;
  auto list_fut = tsuba::FileListAsync(dir, &files);
  GALOIS_LOG_ASSERT(list_fut.valid());

  if (auto res = list_fut.get(); !res) {
    return res.error();
  }
  return files;
}

bool
ContainsValidMetaFile(const std::string& dir) {
  auto list_res = FileList(dir);
  if (!list_res) {
    GALOIS_LOG_DEBUG(
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

galois::Result<galois::Uri>
FindLatestMetaFile(const galois::Uri& name) {
  assert(!tsuba::RDGMeta::IsMetaUri(name));
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
    GALOIS_LOG_DEBUG("failed: could not find meta file in {}", name);
    return tsuba::ErrorCode::InvalidArgument;
  }
  return name.Join(found_meta);
}

}  // namespace

galois::Result<tsuba::RDGHandle>
tsuba::Open(const std::string& rdg_name, uint32_t flags) {
  if (!OpenFlagsValid(flags)) {
    GALOIS_LOG_ERROR("invalid value for flags ({:#x})", flags);
    return ErrorCode::InvalidArgument;
  }

  auto uri_res = galois::Uri::Make(rdg_name);
  if (!uri_res) {
    return uri_res.error();
  }
  galois::Uri uri = std::move(uri_res.value());

  if (RDGMeta::IsMetaUri(uri)) {
    GALOIS_LOG_DEBUG(
        "failed: {} is probably a literal rdg file and not suited for open",
        uri);
    return ErrorCode::InvalidArgument;
  }

  if (!RDGMeta::IsMetaUri(uri)) {
    // try to be helpful and look for RDGs that we don't know about
    if (auto res = RegisterIfAbsent(uri.string()); !res) {
      GALOIS_LOG_DEBUG("failed to auto-register: {}", res.error());
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

galois::Result<void>
tsuba::Close(RDGHandle handle) {
  delete handle.impl_;
  return galois::ResultSuccess();
}

galois::Result<void>
tsuba::Create(const std::string& name) {
  auto uri_res = galois::Uri::Make(name);
  if (!uri_res) {
    return uri_res.error();
  }
  galois::Uri uri = std::move(uri_res.value());

  assert(!RDGMeta::IsMetaUri(uri));
  // the default construction is the empty RDG
  tsuba::RDGMeta meta{};

  galois::CommBackend* comm = Comm();
  if (comm->ID == 0) {
    if (ContainsValidMetaFile(name)) {
      GALOIS_LOG_ERROR(
          "unable to create {}: path already contains a valid meta file", name);
      return ErrorCode::InvalidArgument;
    }
    std::string s = meta.ToJsonString();
    if (auto res = tsuba::FileStore(
            tsuba::RDGMeta::FileName(uri, meta.version()).string(),
            reinterpret_cast<const uint8_t*>(s.data()), s.size());
        !res) {
      GALOIS_LOG_ERROR("failed to store RDG file");
      comm->NotifyFailure();
      return res.error();
    }
  }

  // NS handles MPI coordination
  if (auto res = tsuba::NS()->CreateIfAbsent(uri, meta); !res) {
    GALOIS_LOG_ERROR("failed to create RDG name");
    return res.error();
  }

  return galois::ResultSuccess();
}

galois::Result<void>
tsuba::RegisterIfAbsent(const std::string& name) {
  auto uri_res = galois::Uri::Make(name);
  if (!uri_res) {
    return uri_res.error();
  }
  galois::Uri uri = std::move(uri_res.value());

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

galois::Result<void>
tsuba::Forget(const std::string& name) {
  auto uri_res = galois::Uri::Make(name);
  if (!uri_res) {
    return uri_res.error();
  }
  galois::Uri uri = std::move(uri_res.value());

  if (RDGMeta::IsMetaUri(uri)) {
    GALOIS_LOG_DEBUG("uri does not look like a graph name (ends in meta)");
    return ErrorCode::InvalidArgument;
  }

  // NS ensures only host 0 creates
  auto res = tsuba::NS()->Delete(uri);
  return res;
}

galois::Result<tsuba::RDGStat>
tsuba::Stat(const std::string& rdg_name) {
  auto uri_res = galois::Uri::Make(rdg_name);
  if (!uri_res) {
    return uri_res.error();
  }
  galois::Uri uri = std::move(uri_res.value());

  if (!RDGMeta::IsMetaUri(uri)) {
    // try to be helpful and look for RDGs that we don't know about
    if (auto res = RegisterIfAbsent(uri.string()); !res) {
      GALOIS_LOG_DEBUG("failed to auto-register: {}", res.error());
      return res.error();
    }
  }

  auto rdg_res = RDGMeta::Make(uri);
  if (!rdg_res) {
    if (rdg_res.error() == galois::ErrorCode::JsonParseFailed) {
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

/// get a name server client based on the environment. If GALOIS_NS_HOST and
/// GALOIS_NS_PORT are set, connect to HTTP server, else use the memory client
/// (memory client provides no cross instance guarantees, good only for testing)
galois::Result<std::unique_ptr<tsuba::NameServerClient>>
tsuba::GetNameServerClient() {
  std::string url;

  galois::GetEnv("GALOIS_NS_URL", &url);

  if (url.empty()) {
    GALOIS_LOG_WARN(
        "name server not configured, no consistency guarantees "
        "between Katana instances");
    return std::make_unique<MemoryNameServerClient>();
  }
  return HttpNameServerClient::Make(url);
}

galois::Result<void>
tsuba::Init(galois::CommBackend* comm, tsuba::NameServerClient* ns) {
  tsuba::Preload();
  galois::InitBacktrace(comm->ID);
  return GlobalState::Init(comm, ns);
}

galois::Result<void>
tsuba::Init() {
  return Init(&default_comm_backend, &default_ns_client);
}

galois::Result<void>
tsuba::Fini() {
  auto r = GlobalState::Fini();
  tsuba::PreloadFini();
  return r;
}
