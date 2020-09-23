#include "tsuba/tsuba.h"

#include "GlobalState.h"
#include "galois/CommBackend.h"
#include "galois/GetEnv.h"
#include "tsuba/HttpNameServerClient.h"
#include "tsuba/MemoryNameServerClient.h"

static galois::NullCommBackend default_comm_backend;
static tsuba::MemoryNameServerClient default_ns_client;

/// get a name server client based on the environment. If GALOIS_NS_HOST and
/// GALOIS_NS_PORT are set, connect to HTTP server, else use the memory client
/// (memory client provides no cross instance guarantees, good only for testing)
galois::Result<std::unique_ptr<tsuba::NameServerClient>>
tsuba::GetNameServerClient() {
  std::string host;
  int port = 0;

  galois::GetEnv("GALOIS_NS_HOST", &host);
  galois::GetEnv("GALOIS_NS_PORT", &port);

  if (host.empty()) {
    GALOIS_LOG_WARN(
        "name server not configured, no consistency guarantees "
        "between Katana instances");
    return std::make_unique<MemoryNameServerClient>();
  }
  return HttpNameServerClient::Make(host, port);
}

galois::Result<void>
tsuba::Init(galois::CommBackend* comm, tsuba::NameServerClient* ns) {
  return GlobalState::Init(comm, ns);
}

galois::Result<void>
tsuba::Init() {
  return Init(&default_comm_backend, &default_ns_client);
}

galois::Result<void>
tsuba::Fini() {
  return GlobalState::Fini();
}
