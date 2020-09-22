#ifndef GALOIS_LIBTSUBA_TSUBA_MEMORYNAMESERVERCLIENT_H_
#define GALOIS_LIBTSUBA_TSUBA_MEMORYNAMESERVERCLIENT_H_

#include <cstdint>
#include <memory>
#include <mutex>
#include <string>

#include "galois/Result.h"
#include "tsuba/NameServerClient.h"
#include "tsuba/RDG.h"

namespace tsuba {

class MemoryNameServerClient : public NameServerClient {
  std::mutex mutex_;
  std::unordered_map<std::string, RDGMeta> server_state_;

  galois::Result<RDGMeta> lookup(const std::string& key);

public:
  galois::Result<RDGMeta> Get(const std::string& rdg_name) override;

  galois::Result<void> Create(
      const std::string& rdg_name, const RDGMeta& meta) override;

  galois::Result<void> Update(
      const std::string& rdg_name, uint64_t old_version,
      const RDGMeta& meta) override;

  galois::Result<void> CheckHealth() override;
};

}  // namespace tsuba

#endif
