#ifndef GALOIS_LIBTSUBA_TSUBA_TSUBA_H_
#define GALOIS_LIBTSUBA_TSUBA_TSUBA_H_

#include "galois/config.h"
#include "galois/Result.h"
#include "galois/CommBackend.h"

namespace tsuba {

// Setup and tear down
GALOIS_EXPORT galois::Result<void> Init(galois::CommBackend* comm);
GALOIS_EXPORT galois::Result<void> Init();

GALOIS_EXPORT galois::Result<void> Fini();

} // namespace tsuba

#endif
