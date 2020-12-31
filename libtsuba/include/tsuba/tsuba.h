#ifndef GALOIS_LIBTSUBA_TSUBA_TSUBA_H_
#define GALOIS_LIBTSUBA_TSUBA_TSUBA_H_

#include <memory>

#include "galois/CommBackend.h"
#include "galois/Result.h"
#include "galois/config.h"

namespace tsuba {

class RDGHandleImpl;

/// RDGHandle is an opaque indentifier for an RDG.
struct RDGHandle {
  RDGHandleImpl* impl_{};
};

/// RDGFile wraps an RDGHandle to close the handle when RDGFile is destroyed.
class GALOIS_EXPORT RDGFile {
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

GALOIS_EXPORT galois::Result<RDGHandle> Open(
    const std::string& rdg_name, uint32_t flags);

GALOIS_EXPORT galois::Result<RDGHandle> Open(
    const std::string& rdg_name, uint64_t version, uint32_t flags);

/// Close an RDGHandle object
GALOIS_EXPORT galois::Result<void> Close(RDGHandle handle);

/// Create an RDG storage location
/// \param name is storage location prefix that will be used to store the RDG
GALOIS_EXPORT galois::Result<void> Create(const std::string& name);

/// RegisterIfAbsent registers a previously created RDG and attaches it to the
/// namespace; infer the version by examining files in name. If the RDG is
/// already registered, this operation is a noop.
///
/// \param name is storage location prefix that the RDG is stored in
GALOIS_EXPORT galois::Result<void> RegisterIfAbsent(const std::string& name);

/// Forget an RDG, detaching it from the namespace
/// \param name is storage location prefix that the RDG is stored in
GALOIS_EXPORT galois::Result<void> Forget(const std::string& name);

struct GALOIS_EXPORT RDGStat {
  uint64_t num_hosts{0};
  uint32_t policy_id{0};
  bool transpose{false};
};

/// Get Information about the graph
GALOIS_EXPORT galois::Result<RDGStat> Stat(const std::string& rdg_name);

// Setup and tear down
GALOIS_EXPORT galois::Result<void> Init(galois::CommBackend* comm);
GALOIS_EXPORT galois::Result<void> Init();

GALOIS_EXPORT galois::Result<void> Fini();

}  // namespace tsuba

#endif
