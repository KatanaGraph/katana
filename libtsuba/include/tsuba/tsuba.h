#ifndef GALOIS_LIBTSUBA_TSUBA_TSUBA_H_
#define GALOIS_LIBTSUBA_TSUBA_TSUBA_H_

#include <memory>

#include "galois/CommBackend.h"
#include "galois/Result.h"
#include "galois/config.h"
#include "tsuba/NameServerClient.h"

namespace tsuba {

/// get a name server client based on the environment. If GALOIS_NS_HOST and
/// GALOIS_NS_PORT are set, connect to HTTP server, else use the memory client
/// (memory client provides no cross instance guarantees, good only for testing)
GALOIS_EXPORT galois::Result<std::unique_ptr<NameServerClient>>
GetNameServerClient();

// Setup and tear down
GALOIS_EXPORT galois::Result<void> Init(
    galois::CommBackend* comm, NameServerClient* ns);

GALOIS_EXPORT galois::Result<void> Init();

GALOIS_EXPORT galois::Result<void> Fini();

}  // namespace tsuba

#endif
