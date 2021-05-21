#include "MemoryNameServerClient.h"

#include <cassert>
#include <future>
#include <regex>

#include <curl/curl.h>
#include <nlohmann/json.hpp>

#include "GlobalState.h"
#include "katana/Env.h"
#include "katana/JSON.h"
#include "katana/Logging.h"
#include "katana/Result.h"
#include "tsuba/Errors.h"

namespace tsuba {

katana::Result<RDGMeta>
MemoryNameServerClient::lookup(const std::string& key) {
  std::lock_guard<std::mutex> lock(mutex_);
  auto it = server_state_.find(key);
  if (it == server_state_.end()) {
    return ErrorCode::NotFound;
  }
  return it->second;
}

katana::Result<RDGMeta>
MemoryNameServerClient::Get(const katana::Uri& rdg_name) {
  return lookup(rdg_name.Encode());
}

katana::Result<void>
MemoryNameServerClient::CreateIfAbsent(
    const katana::Uri& rdg_name, const RDGMeta& meta) {
  std::string key = rdg_name.Encode();

  // CreateIfAbsent, Delete and Update are collective operations
  Comm()->Barrier();

  std::lock_guard<std::mutex> lock(mutex_);

  if (server_state_.find(key) == server_state_.end()) {
    server_state_.emplace(key, meta);
  } else if (server_state_[key].version() != meta.version()) {
    KATANA_LOG_WARN(
        "mismatched versions {} != {}", server_state_[key].version(),
        meta.version());
  }

  return katana::ResultSuccess();
}

katana::Result<void>
MemoryNameServerClient::Delete(const katana::Uri& rdg_name) {
  Comm()->Barrier();

  std::lock_guard<std::mutex> lock(mutex_);
  auto it = server_state_.find(rdg_name.Encode());
  if (it == server_state_.end()) {
    return ErrorCode::NotFound;
  }
  server_state_.erase(it);
  return katana::ResultSuccess();
}

katana::Result<void>
MemoryNameServerClient::Update(
    const katana::Uri& rdg_name, uint64_t old_version, const RDGMeta& meta) {
  Comm()->Barrier();

  if (old_version >= meta.version()) {
    return ErrorCode::InvalidArgument;
  }
  std::lock_guard<std::mutex> lock(mutex_);
  auto it = server_state_.find(rdg_name.Encode());
  if (it == server_state_.end()) {
    return ErrorCode::NotFound;
  }
  if (it->second.version() != old_version) {
    return ErrorCode::BadVersion;
  }
  server_state_.erase(it);
  server_state_.emplace(rdg_name.Encode(), meta);
  return katana::ResultSuccess();
}

katana::Result<void>
MemoryNameServerClient::CheckHealth() {
  return katana::ResultSuccess();
}

}  // namespace tsuba
