#ifndef KATANA_LIBTSUBA_TSUBA_PRELOAD_H_
#define KATANA_LIBTSUBA_TSUBA_PRELOAD_H_

#include "katana/config.h"

namespace tsuba {

/// Preload allows for linker-level customization before the tsuba library is
/// initialized. Preload is called before the tsuba library initializes itself.
KATANA_EXPORT void Preload();

/// PreloadFini allows for cleanup of any resources created by Preload. It is
/// called after the tsuba library code finalizes itself.
KATANA_EXPORT void PreloadFini();

}  // namespace tsuba

#endif
