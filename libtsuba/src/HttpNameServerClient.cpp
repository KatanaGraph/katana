#include "tsuba/HttpNameServerClient.h"

#include <cassert>
#include <future>
#include <regex>

#include "GlobalState.h"
#include "galois/Http.h"
#include "galois/Logging.h"
#include "galois/Result.h"
#include "tsuba/Errors.h"

using json = nlohmann::json;

namespace {

struct HttpResponse {
  std::string status;
  std::string error;
};

void
from_json(const json& j, HttpResponse& resp) {
  if (j.find("status") != j.end()) {
    j.at("status").get_to(resp.status);
  }
  if (j.find("error") != j.end()) {
    j.at("error").get_to(resp.status);
  }
}

}  // namespace

namespace tsuba {

galois::Result<std::string>
HttpNameServerClient::BuildUrl(const galois::Uri& rdg_name) {
  return prefix_ + "rdgs/" + rdg_name.Encode();
}

galois::Result<void>
HttpNameServerClient::CheckHealth() {
  auto health_res =
      galois::HttpGetJson<HttpResponse>(prefix_ + "health-status");
  if (!health_res) {
    return health_res.error();
  }
  HttpResponse health = std::move(health_res.value());
  if (health.status != "ok") {
    GALOIS_LOG_ERROR("name server reports status {}", health.status);
    return ErrorCode::TODO;
  }
  return galois::ResultSuccess();
}

galois::Result<RDGMeta>
HttpNameServerClient::Get(const galois::Uri& rdg_name) {
  auto uri_res = BuildUrl(rdg_name);
  if (!uri_res) {
    return uri_res.error();
  }
  auto meta_res = galois::HttpGetJson<RDGMeta>(uri_res.value());
  if (meta_res) {
    meta_res.value().set_dir(rdg_name);
  }
  return meta_res;
}

galois::Result<void>
HttpNameServerClient::Create(const galois::Uri& rdg_name, const RDGMeta& meta) {
  // TODO(thunt) we check ID here because MemoryNameServer needs to be able to
  // store separate copies on all hosts for testing (fix it)
  return OneHostOnly([&]() -> galois::Result<void> {
    auto uri_res = BuildUrl(rdg_name);
    if (!uri_res) {
      return uri_res.error();
    }
    auto resp_res =
        galois::HttpPostJson<RDGMeta, HttpResponse>(uri_res.value(), meta);
    if (!resp_res) {
      return resp_res.error();
    }
    HttpResponse resp = std::move(resp_res.value());
    if (resp.status != "ok") {
      GALOIS_LOG_DEBUG("request succeeded but reported error {}", resp.error);
      return ErrorCode::TODO;
    }
    return galois::ResultSuccess();
  });
}

galois::Result<void>
HttpNameServerClient::Delete(const galois::Uri& rdg_name) {
  // TODO(thunt) we check ID here because MemoryNameServer needs to be able to
  // store separate copies on all hosts for testing (fix it)
  return OneHostOnly([&]() -> galois::Result<void> {
    auto uri_res = BuildUrl(rdg_name);
    if (!uri_res) {
      return uri_res.error();
    }
    auto resp_res = galois::HttpDeleteJson<HttpResponse>(uri_res.value());
    if (!resp_res) {
      return resp_res.error();
    }

    HttpResponse resp = std::move(resp_res.value());
    if (resp.status != "ok") {
      GALOIS_LOG_DEBUG("request succeeded but reported error {}", resp.error);
      return ErrorCode::TODO;
    }
    return galois::ResultSuccess();
  });
}

galois::Result<void>
HttpNameServerClient::Update(
    const galois::Uri& rdg_name, uint64_t old_version, const RDGMeta& meta) {
  // TODO(thunt) we check ID here because MemoryNameServer needs to be able to
  // store separate copies on all hosts for testing (fix it)
  return OneHostOnly([&]() -> galois::Result<void> {
    auto uri_res = BuildUrl(rdg_name);
    if (!uri_res) {
      return uri_res.error();
    }
    auto query_string = fmt::format("?expected-version={}", old_version);

    auto resp_res = galois::HttpPutJson<RDGMeta, HttpResponse>(
        uri_res.value() + query_string, meta);
    if (!resp_res) {
      return resp_res.error();
    }

    HttpResponse resp = std::move(resp_res.value());
    if (resp.status != "ok") {
      GALOIS_LOG_DEBUG("request succeeded but reported error {}", resp.error);
      return ErrorCode::TODO;
    }
    return galois::ResultSuccess();
  });
}

galois::Result<std::unique_ptr<NameServerClient>>
HttpNameServerClient::Make(std::string_view url) {
  // HttpInit is idempotent
  if (auto res = galois::HttpInit(); !res) {
    return res.error();
  }
  return std::unique_ptr<NameServerClient>(new HttpNameServerClient(url));
}

}  // namespace tsuba
