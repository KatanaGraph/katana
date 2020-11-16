#ifndef GALOIS_LIBTSUBA_RDGHANDLEIMPL_H_
#define GALOIS_LIBTSUBA_RDGHANDLEIMPL_H_

#include <cstdint>

#include "galois/Uri.h"
#include "tsuba/RDGMeta.h"
#include "tsuba/tsuba.h"

namespace tsuba {

struct RDGHandleImpl {
  // Property paths are relative rdg_meta.dir_
  galois::Uri partition_path;
  uint32_t flags;
  RDGMeta rdg_meta;

  /// Perform some checks on assumed invariants
  galois::Result<void> Validate() const;
  constexpr bool AllowsReadPartial() const { return flags & kReadPartial; }
  constexpr bool AllowsRead() const { return !AllowsReadPartial(); }
  constexpr bool AllowsWrite() const { return flags & kReadWrite; }
};

}  // namespace tsuba

#endif
