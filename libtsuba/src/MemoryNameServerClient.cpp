#include "tsuba/MemoryNameServerClient.h"

#include <cassert>
#include <future>
#include <regex>

#include <curl/curl.h>
#include <nlohmann/json.hpp>

#include "GlobalState.h"
#include "galois/Env.h"
#include "galois/JSON.h"
#include "galois/Logging.h"
#include "galois/Result.h"
#include "tsuba/Errors.h"

namespace tsuba {

galois::Result<RDGMeta>
MemoryNameServerClient::lookup(const std::string& key) {
  std::lock_guard<std::mutex> lock(mutex_);
  auto it = server_state_.find(key);
  if (it == server_state_.end()) {
    return ErrorCode::NotFound;
  }
  return it->second;
}

galois::Result<RDGMeta>
MemoryNameServerClient::Get(const galois::Uri& rdg_name) {
  return lookup(rdg_name.Encode());
}

galois::Result<void>
MemoryNameServerClient::CreateIfAbsent(
    const galois::Uri& rdg_name, const RDGMeta& meta) {
  std::string key = rdg_name.Encode();

  // CreateIfAbsent, Delete and Update are collective operations
  Comm()->Barrier();

  std::lock_guard<std::mutex> lock(mutex_);

  if (server_state_.find(key) == server_state_.end()) {
    server_state_.emplace(key, meta);
  } else if (server_state_[key].version() != meta.version()) {
    GALOIS_LOG_DEBUG(
        "mismatched versions {} != {}", server_state_[key].version(),
        meta.version());
    return ErrorCode::TODO;
  }

  return galois::ResultSuccess();
}

galois::Result<void>
MemoryNameServerClient::Delete(const galois::Uri& rdg_name) {
  Comm()->Barrier();

  std::lock_guard<std::mutex> lock(mutex_);
  auto it = server_state_.find(rdg_name.Encode());
  if (it == server_state_.end()) {
    return ErrorCode::NotFound;
  }
  server_state_.erase(it);
  return galois::ResultSuccess();
}

galois::Result<void>
MemoryNameServerClient::Update(
    const galois::Uri& rdg_name, uint64_t old_version, const RDGMeta& meta) {
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
  return galois::ResultSuccess();
}

galois::Result<void>
MemoryNameServerClient::CheckHealth() {
  return galois::ResultSuccess();
}

}  // namespace tsuba
