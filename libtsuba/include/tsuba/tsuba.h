#ifndef GALOIS_LIBTSUBA_TSUBA_TSUBA_H_
#define GALOIS_LIBTSUBA_TSUBA_TSUBA_H_

#include "galois/CommBackend.h"
#include "galois/Result.h"
#include "galois/config.h"
#include "tsuba/NameServerClient.h"

namespace tsuba {

// Setup and tear down
GALOIS_EXPORT galois::Result<void> Init(
    galois::CommBackend* comm, tsuba::NameServerClient* ns);

GALOIS_EXPORT galois::Result<void> Init();

GALOIS_EXPORT galois::Result<void> Fini();

}  // namespace tsuba

#endif
