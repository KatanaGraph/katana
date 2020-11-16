#ifndef GALOIS_LIBTSUBA_TSUBA_RDGINTERNAL_H_
#define GALOIS_LIBTSUBA_TSUBA_RDGINTERNAL_H_

#include <cstdint>
#include <set>
#include <string>

#include <arrow/api.h>

#include "galois/Result.h"
#include "galois/Uri.h"
#include "galois/config.h"

namespace tsuba::internal {

// Used for garbage collection
// Return all file names that store data for this handle
GALOIS_EXPORT galois::Result<std::set<std::string>> FileNames(
    const galois::Uri& dir, uint64_t version);

}  // namespace tsuba::internal

#endif
