#include "tsuba/tsuba.h"

#include "GlobalState.h"
#include "galois/CommBackend.h"
#include "galois/GetEnv.h"
#include "tsuba/HttpNameServerClient.h"
#include "tsuba/MemoryNameServerClient.h"
#include "tsuba/Preload.h"

static galois::NullCommBackend default_comm_backend;
static tsuba::MemoryNameServerClient default_ns_client;

/// get a name server client based on the environment. If GALOIS_NS_HOST and
/// GALOIS_NS_PORT are set, connect to HTTP server, else use the memory client
/// (memory client provides no cross instance guarantees, good only for testing)
galois::Result<std::unique_ptr<tsuba::NameServerClient>>
tsuba::GetNameServerClient() {
  std::string url;

  galois::GetEnv("GALOIS_NS_URL", &url);

  if (url.empty()) {
    GALOIS_LOG_WARN(
        "name server not configured, no consistency guarantees "
        "between Katana instances");
    return std::make_unique<MemoryNameServerClient>();
  }
  return HttpNameServerClient::Make(url);
}

galois::Result<void>
tsuba::Init(galois::CommBackend* comm, tsuba::NameServerClient* ns) {
  tsuba::Preload();
  return GlobalState::Init(comm, ns);
}

galois::Result<void>
tsuba::Init() {
  return Init(&default_comm_backend, &default_ns_client);
}

galois::Result<void>
tsuba::Fini() {
  auto r = GlobalState::Fini();
  tsuba::PreloadFini();
  return r;
}
