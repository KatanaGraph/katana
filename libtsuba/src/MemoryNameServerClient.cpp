#include "tsuba/MemoryNameServerClient.h"

#include <cassert>
#include <future>
#include <regex>

#include <curl/curl.h>
#include <nlohmann/json.hpp>

#include "galois/GetEnv.h"
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
  // take the lock in lookup to avoid deadlock since Register will call Create
  auto meta_res = lookup(rdg_name.Encode());
  if (!meta_res && meta_res.error() == ErrorCode::NotFound) {
    // The real NameServer is a service that outlives engine instances;
    // emulate that by trying to register graphs that we don't know about.
    GALOIS_LOG_DEBUG("attempting to auto-register rdg: {}", rdg_name.string());
    if (auto res = Register(rdg_name.string()); !res) {
      return res.error();
    }
    meta_res = lookup(rdg_name.Encode());
  }
  return meta_res;
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
MemoryNameServerClient::Update(
    const galois::Uri& rdg_name, uint64_t old_version, const RDGMeta& meta) {
  if (old_version >= meta.version_) {
    return ErrorCode::InvalidArgument;
  }
  std::lock_guard<std::mutex> lock(mutex_);
  auto it = server_state_.find(rdg_name.Encode());
  if (it == server_state_.end()) {
    return ErrorCode::NotFound;
  }
  if (it->second.version_ != old_version) {
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
