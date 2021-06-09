#ifndef KATANA_LIBTSUBA_TSUBA_NAMESERVERCLIENT_H_
#define KATANA_LIBTSUBA_TSUBA_NAMESERVERCLIENT_H_

#include "katana/Result.h"
#include "katana/Uri.h"

namespace tsuba {

class RDGMeta;

class KATANA_EXPORT NameServerClient {
public:
  NameServerClient() = default;
  NameServerClient(NameServerClient&& no_move) = delete;
  NameServerClient& operator=(NameServerClient&& no_move) = delete;
  NameServerClient(const NameServerClient& no_copy) = delete;
  NameServerClient& operator=(const NameServerClient& no_copy) = delete;
  virtual ~NameServerClient();

  virtual katana::Result<RDGMeta> Get(const katana::Uri& rdg_name) = 0;

  /// CreateIfAbsent creates a name server entry if it is not already
  /// present. If the name is already created and its version matches meta,
  /// this function returns sucess; otherwise, it returns an error.
  ///
  /// This is a collective operation.
  virtual katana::Result<void> CreateIfAbsent(
      const katana::Uri& rdg_name, const RDGMeta& meta) = 0;

  /// Delete removes a name server entry.
  ///
  /// This is a collective operation.
  virtual katana::Result<void> Delete(const katana::Uri& rdg_name) = 0;

  /// Update increments the latest version of a name.
  ///
  /// This is a collective operation.
  virtual katana::Result<void> Update(
      const katana::Uri& rdg_name, uint64_t old_version,
      const RDGMeta& meta) = 0;

  virtual katana::Result<void> CheckHealth() = 0;
};

/// SetNameServerClientCB sets the callback that tsuba uses when
/// the user requests a NameServerClient via `tsuba::GetNameServerClient`
KATANA_EXPORT void SetMakeNameServerClientCB(
    std::function<katana::Result<std::unique_ptr<tsuba::NameServerClient>>()>
        cb);

/// ClearMakeNameServerClientCB clears the callback back to the default.
/// This must be called if the previously registere callback is being unloaded
/// during plugin finalization.
KATANA_EXPORT void ClearMakeNameServerClientCB();

}  // namespace tsuba

#endif
