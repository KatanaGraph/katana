#ifndef GALOIS_LIBTSUBA_TSUBA_HTTPNAMESERVERCLIENT_H_
#define GALOIS_LIBTSUBA_TSUBA_HTTPNAMESERVERCLIENT_H_

#include <cstdint>
#include <memory>
#include <string>

#include "galois/Result.h"
#include "galois/Uri.h"
#include "tsuba/NameServerClient.h"
#include "tsuba/RDG.h"

namespace tsuba {

class GALOIS_EXPORT HttpNameServerClient : public NameServerClient {
  std::string prefix_;
  galois::Result<std::string> BuildUrl(const galois::Uri& rdg_name);

  HttpNameServerClient(std::string_view url)
      : prefix_(fmt::format("{}/apiV1/", url)) {}

public:
  galois::Result<RDGMeta> Get(const galois::Uri& rdg_name) override;

  galois::Result<void> Create(
      const galois::Uri& rdg_name, const RDGMeta& meta) override;

  galois::Result<void> Delete(const galois::Uri& rdg_name) override;

  galois::Result<void> Update(
      const galois::Uri& rdg_name, uint64_t old_version,
      const RDGMeta& meta) override;

  galois::Result<void> CheckHealth() override;

  static galois::Result<std::unique_ptr<tsuba::NameServerClient>> Make(
      std::string_view url);
};

}  // namespace tsuba

#endif
