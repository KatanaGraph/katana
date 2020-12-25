#ifndef GALOIS_LIBTSUBA_RDGHANDLEIMPL_H_
#define GALOIS_LIBTSUBA_RDGHANDLEIMPL_H_

#include <cstdint>

#include "RDGMeta.h"
#include "galois/Uri.h"
#include "tsuba/tsuba.h"

namespace tsuba {

class RDGHandleImpl {
public:
  RDGHandleImpl(uint32_t flags, RDGMeta&& rdg_meta)
      : flags_(flags), rdg_meta_(std::move(rdg_meta)) {}

  /// Perform some checks on assumed invariants
  galois::Result<void> Validate() const;
  constexpr bool AllowsRead() const { return true; }
  constexpr bool AllowsWrite() const { return flags_ & kReadWrite; }

  //
  // Accessors and Mutators
  //
  const RDGMeta& rdg_meta() const { return rdg_meta_; }
  void set_rdg_meta(RDGMeta&& rdg_meta) { rdg_meta_ = std::move(rdg_meta); }

private:
  uint32_t flags_;
  RDGMeta rdg_meta_;
};

}  // namespace tsuba

#endif
