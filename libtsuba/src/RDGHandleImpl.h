#ifndef KATANA_LIBTSUBA_RDGHANDLEIMPL_H_
#define KATANA_LIBTSUBA_RDGHANDLEIMPL_H_

#include <cstdint>

#include "katana/URI.h"
#include "tsuba/RDGManifest.h"
#include "tsuba/tsuba.h"

namespace tsuba {

class RDGHandleImpl {
public:
  RDGHandleImpl(uint32_t flags, RDGManifest&& rdg_manifest)
      : flags_(flags), rdg_manifest_(std::move(rdg_manifest)) {}

  /// Perform some checks on assumed invariants
  katana::Result<void> Validate() const;
  constexpr bool AllowsRead() const { return true; }
  constexpr bool AllowsWrite() const { return flags_ & kReadWrite; }

  //
  // Accessors and Mutators
  //
  const RDGManifest& rdg_manifest() const { return rdg_manifest_; }
  void set_rdg_manifest(RDGManifest&& rdg_manifest) {
    rdg_manifest_ = std::move(rdg_manifest);
  }
  void set_viewtype(const std::string v) { rdg_manifest_.set_viewtype(v); }

private:
  uint32_t flags_;
  RDGManifest rdg_manifest_;
};

}  // namespace tsuba

#endif
