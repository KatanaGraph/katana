#include "NameServerClient.h"

#include <future>
#include <regex>
#include <cassert>

#include <curl/curl.h>
#include <nlohmann/json.hpp>

#include "galois/GetEnv.h"
#include "galois/JSON.h"
#include "galois/Logging.h"
#include "galois/Result.h"
#include "tsuba/Errors.h"

namespace tsuba {

galois::Result<RDGMeta>
DummyTestNameServerClient::lookup(const std::string& key) {
  std::lock_guard<std::mutex> lock(mutex_);
  auto it = server_state_.find(key);
  if (it == server_state_.end()) {
    return ErrorCode::NotFound;
  }
  return it->second;
}

galois::Result<RDGMeta>
DummyTestNameServerClient::Get(const std::string& rdg_name) {
  // take the lock in lookup to avoid deadlock since Register will call Create
  auto meta_res = lookup(rdg_name);
  if (!meta_res && meta_res.error() == ErrorCode::NotFound) {
    // The real NameServer is a service that outlives engine instances;
    // emulate that by trying to register graphs that we don't know about.
    GALOIS_LOG_DEBUG("attempting to auto-register rdg: {}", rdg_name);
    if (auto res = Register(rdg_name); !res) {
      return res.error();
    }
    meta_res = lookup(rdg_name);
  }
  return meta_res;
}

galois::Result<void>
DummyTestNameServerClient::Create(const std::string& rdg_name,
                                  const RDGMeta& meta) {
  std::lock_guard<std::mutex> lock(mutex_);
  bool good                   = false;
  std::tie(std::ignore, good) = server_state_.emplace(rdg_name, meta);
  if (!good) {
    return ErrorCode::Exists;
  }
  return galois::ResultSuccess();
}

galois::Result<void>
DummyTestNameServerClient::Update(const std::string& rdg_name,
                                  uint64_t old_version, const RDGMeta& meta) {
  if (old_version >= meta.version_) {
    return ErrorCode::InvalidArgument;
  }
  std::lock_guard<std::mutex> lock(mutex_);
  auto it = server_state_.find(rdg_name);
  if (it == server_state_.end()) {
    return ErrorCode::NotFound;
  }
  if (it->second.version_ != old_version) {
    return ErrorCode::BadVersion;
  }
  server_state_.erase(it);
  server_state_.emplace(rdg_name, meta);
  return galois::ResultSuccess();
}

galois::Result<void> DummyTestNameServerClient::CheckHealth() {
  return galois::ResultSuccess();
}

galois::Result<std::unique_ptr<NameServerClient>>
DummyTestNameServerClient::Make() {
  return std::unique_ptr<NameServerClient>(new DummyTestNameServerClient());
}

} // namespace tsuba

galois::Result<std::unique_ptr<tsuba::NameServerClient>>
tsuba::ConnectToNameServer() {
  std::string host;
  int port = 0;

  galois::GetEnv("GALOIS_NS_HOST", &host);
  galois::GetEnv("GALOIS_NS_PORT", &port);

  if (host.empty()) {
    GALOIS_LOG_WARN("name server not configured, no consistency guarantees "
                    "between Katana instances");
    return DummyTestNameServerClient::Make();
  }
  GALOIS_LOG_DEBUG("connecting to nameserver {}:{}", host, port);
  return ErrorCode::NotImplemented;
}
