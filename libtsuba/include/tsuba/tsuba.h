#ifndef KATANA_LIBTSUBA_TSUBA_TSUBA_H_
#define KATANA_LIBTSUBA_TSUBA_TSUBA_H_

#include <memory>

#include "katana/CommBackend.h"
#include "katana/Result.h"
#include "katana/Uri.h"
#include "katana/config.h"

namespace tsuba {

class RDGHandleImpl;

/// RDGHandle is an opaque indentifier for an RDG.
struct RDGHandle {
  RDGHandleImpl* impl_{};
};

/// RDGFile wraps an RDGHandle to close the handle when RDGFile is destroyed.
class KATANA_EXPORT RDGFile {
  RDGHandle handle_;

public:
  explicit RDGFile(RDGHandle handle) : handle_(handle) {}

  RDGFile(const RDGFile&) = delete;
  RDGFile& operator=(const RDGFile&) = delete;

  RDGFile(RDGFile&& f) noexcept : handle_(f.handle_) {}
  RDGFile& operator=(RDGFile&& f) noexcept {
    std::swap(handle_, f.handle_);
    return *this;
  }

  operator RDGHandle&() { return handle_; }

  ~RDGFile();
};

// acceptable values for Open's flags
constexpr uint32_t kReadOnly = 0;
constexpr uint32_t kReadWrite = 1;
constexpr bool
OpenFlagsValid(uint32_t flags) {
  return (flags & ~(kReadOnly | kReadWrite)) == 0;
}

KATANA_EXPORT katana::Result<RDGHandle> Open(
    const std::string& rdg_name, uint32_t flags);

KATANA_EXPORT katana::Result<RDGHandle> Open(
    const std::string& rdg_name, uint64_t version, uint32_t flags);

/// Generate a new canonically named topology file name in the
/// directory associated with handle. Exported to support
/// out-of-core conversion
KATANA_EXPORT katana::Uri MakeTopologyFileName(RDGHandle handle);

/// Get the storage directory associated with this handle
KATANA_EXPORT katana::Uri GetRDGDir(RDGHandle handle);

/// Close an RDGHandle object
KATANA_EXPORT katana::Result<void> Close(RDGHandle handle);

/// Create an RDG storage location
/// \param name is storage location prefix that will be used to store the RDG
KATANA_EXPORT katana::Result<void> Create(const std::string& name);

/// RegisterIfAbsent registers a previously created RDG and attaches it to the
/// namespace; infer the version by examining files in name. If the RDG is
/// already registered, this operation is a noop.
///
/// \param name is storage location prefix that the RDG is stored in
KATANA_EXPORT katana::Result<void> RegisterIfAbsent(const std::string& name);

/// Forget an RDG, detaching it from the namespace
/// \param name is storage location prefix that the RDG is stored in
KATANA_EXPORT katana::Result<void> Forget(const std::string& name);

struct KATANA_EXPORT RDGStat {
  uint64_t num_hosts{0};
  uint32_t policy_id{0};
  bool transpose{false};
};

/// Get Information about the graph
KATANA_EXPORT katana::Result<RDGStat> Stat(const std::string& rdg_name);

// Setup and tear down
KATANA_EXPORT katana::Result<void> Init(katana::CommBackend* comm);
KATANA_EXPORT katana::Result<void> Init();

KATANA_EXPORT katana::Result<void> Fini();

}  // namespace tsuba

#endif
