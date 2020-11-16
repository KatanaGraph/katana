#ifndef GALOIS_LIBTSUBA_TSUBA_NAMESERVERCLIENT_H_
#define GALOIS_LIBTSUBA_TSUBA_NAMESERVERCLIENT_H_

#include "galois/Result.h"
#include "galois/Uri.h"
#include "tsuba/RDGMeta.h"

namespace tsuba {

class GALOIS_EXPORT NameServerClient {
public:
  NameServerClient() = default;
  NameServerClient(NameServerClient&& no_move) = delete;
  NameServerClient& operator=(NameServerClient&& no_move) = delete;
  NameServerClient(const NameServerClient& no_copy) = delete;
  NameServerClient& operator=(const NameServerClient& no_copy) = delete;
  virtual ~NameServerClient() = default;
  virtual galois::Result<RDGMeta> Get(const galois::Uri& rdg_name) = 0;
  virtual galois::Result<void> Create(
      const galois::Uri& rdg_name, const RDGMeta& meta) = 0;
  virtual galois::Result<void> Delete(const galois::Uri& rdg_name) = 0;
  virtual galois::Result<void> Update(
      const galois::Uri& rdg_name, uint64_t old_version,
      const RDGMeta& meta) = 0;
  virtual galois::Result<void> CheckHealth() = 0;
};

}  // namespace tsuba

#endif
