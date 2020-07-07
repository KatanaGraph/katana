#ifndef GALOIS_LIBTSUBA_TSUBA_TSUBA_H_
#define GALOIS_LIBTSUBA_TSUBA_TSUBA_H_

#include "galois/Result.h"
#include "galois/CommBackend.h"

namespace tsuba {

// Setup and tear down
galois::Result<void> Init(galois::CommBackend* comm);
galois::Result<void> Init();

galois::Result<void> Fini();

} // namespace tsuba

#endif
