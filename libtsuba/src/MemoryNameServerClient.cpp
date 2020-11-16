#include "tsuba/MemoryNameServerClient.h"

#include <cassert>
#include <future>
#include <regex>

#include <curl/curl.h>
#include <nlohmann/json.hpp>

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
MemoryNameServerClient::Create(
    const galois::Uri& rdg_name, const RDGMeta& meta) {
  std::lock_guard<std::mutex> lock(mutex_);
  bool good = false;
  std::tie(std::ignore, good) = server_state_.emplace(rdg_name.Encode(), meta);
  if (!good) {
    return ErrorCode::Exists;
  }
  return galois::ResultSuccess();
}

galois::Result<void>
MemoryNameServerClient::Delete(const galois::Uri& rdg_name) {
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
