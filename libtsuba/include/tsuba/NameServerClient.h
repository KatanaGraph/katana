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

  /// CreateIfAbsent creates a name server entry if it is not already
  /// present. If the name is already created and its version matches meta,
  /// this function returns sucess; otherwise, it returns an error.
  ///
  /// This is a collective operation.
  virtual galois::Result<void> CreateIfAbsent(
      const galois::Uri& rdg_name, const RDGMeta& meta) = 0;

  /// Delete removes a name server entry.
  ///
  /// This is a collective operation.
  virtual galois::Result<void> Delete(const galois::Uri& rdg_name) = 0;

  /// Update increments the latest version of a name.
  ///
  /// This is a collective operation.
  virtual galois::Result<void> Update(
      const galois::Uri& rdg_name, uint64_t old_version,
      const RDGMeta& meta) = 0;

  virtual galois::Result<void> CheckHealth() = 0;
};

}  // namespace tsuba

#endif
