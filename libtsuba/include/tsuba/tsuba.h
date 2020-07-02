#ifndef GALOIS_LIBTSUBA_TSUBA_TSUBA_H_
#define GALOIS_LIBTSUBA_TSUBA_TSUBA_H_

#include "galois/Result.h"

namespace tsuba {

// Setup and tear down
galois::Result<void> Init();
galois::Result<void> Fini();

} // namespace tsuba

#endif
