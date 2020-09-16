#ifndef GALOIS_LIBTSUBA_NAMESERVERCLIENT_H_
#define GALOIS_LIBTSUBA_NAMESERVERCLIENT_H_

#include <cstdint>
#include <memory>
#include <mutex>
#include <string>

#include "galois/Result.h"
#include "tsuba/RDG.h"

namespace tsuba {

class NameServerClient {
public:
  NameServerClient()                           = default;
  NameServerClient(NameServerClient&& move_ok) = default;
  NameServerClient& operator=(NameServerClient&& move_ok) = default;
  NameServerClient(const NameServerClient& no_copy)       = delete;
  NameServerClient& operator=(const NameServerClient& no_copy)     = delete;
  virtual ~NameServerClient()                                      = default;
  virtual galois::Result<RDGMeta> Get(const std::string& rdg_name) = 0;
  virtual galois::Result<void> Create(const std::string& rdg_name,
                                      const RDGMeta& meta)         = 0;
  virtual galois::Result<void> Update(const std::string& rdg_name,
                                      uint64_t old_version,
                                      const RDGMeta& meta)         = 0;
  virtual galois::Result<void> CheckHealth()                       = 0;
};

class DummyTestNameServerClient : NameServerClient {
  std::mutex mutex_;
  std::unordered_map<std::string, RDGMeta> server_state_;

  galois::Result<RDGMeta> lookup(const std::string& key);

public:
  galois::Result<RDGMeta> Get(const std::string& rdg_name) override;

  galois::Result<void> Create(const std::string& rdg_name,
                              const RDGMeta& meta) override;

  galois::Result<void> Update(const std::string& rdg_name, uint64_t old_version,
                              const RDGMeta& meta) override;
  galois::Result<void> CheckHealth() override;

  static galois::Result<std::unique_ptr<NameServerClient>> Make();
};

galois::Result<std::unique_ptr<NameServerClient>> ConnectToNameServer();

} // namespace tsuba

#endif
