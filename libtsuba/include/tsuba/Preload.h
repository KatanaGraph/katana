#ifndef GALOIS_LIBTSUBA_TSUBA_PRELOAD_H_
#define GALOIS_LIBTSUBA_TSUBA_PRELOAD_H_

#include "galois/config.h"

namespace tsuba {

/// Preload allows for linker-level customization before the tsuba library is
/// initialized. Preload is called before the tsuba library initializes itself.
GALOIS_EXPORT void Preload();

/// PreloadFini allows for cleanup of any resources created by Preload. It is
/// called after the tsuba library code finalizes itself.
GALOIS_EXPORT void PreloadFini();

}  // namespace tsuba

#endif
