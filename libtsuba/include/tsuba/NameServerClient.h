#ifndef GALOIS_LIBTSUBA_TSUBA_NAMESERVERCLIENT_H_
#define GALOIS_LIBTSUBA_TSUBA_NAMESERVERCLIENT_H_

#include "galois/Result.h"
#include "tsuba/RDG.h"

namespace tsuba {

class NameServerClient {
public:
  NameServerClient() = default;
  NameServerClient(NameServerClient&& move_ok) = default;
  NameServerClient& operator=(NameServerClient&& move_ok) = default;
  NameServerClient(const NameServerClient& no_copy) = delete;
  NameServerClient& operator=(const NameServerClient& no_copy) = delete;
  virtual ~NameServerClient() = default;
  virtual galois::Result<RDGMeta> Get(const std::string& rdg_name) = 0;
  virtual galois::Result<void> Create(
      const std::string& rdg_name, const RDGMeta& meta) = 0;
  virtual galois::Result<void> Update(
      const std::string& rdg_name, uint64_t old_version,
      const RDGMeta& meta) = 0;
  virtual galois::Result<void> CheckHealth() = 0;
};

}  // namespace tsuba

#endif
