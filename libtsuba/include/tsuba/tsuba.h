#ifndef KATANA_LIBTSUBA_TSUBA_TSUBA_H_
#define KATANA_LIBTSUBA_TSUBA_TSUBA_H_

#include <memory>

#include "katana/CommBackend.h"
#include "katana/RDGVersion.h"
#include "katana/Result.h"
#include "katana/RDGVersion.h"
#include "katana/URI.h"
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
    const std::string& rdg_name, katana::RDGVersion version, uint32_t flags);

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
KATANA_EXPORT katana::Result<void> Create(
    const std::string& name, katana::RDGVersion = katana::RDGVersion(0));

/// @brief Describes properties of RDGView
/// The RDGView will describe will identify the view-type, the arguments used to
/// create it, where it is stored, and the properties of the partioning strategy
/// used to distribute its data across the hosts which will load it.
struct KATANA_EXPORT RDGView {
  katana::RDGVersion view_version{katana::RDGVersion(0)};
  std::string view_type;
  std::string view_args;
  std::string view_path;
  uint64_t num_partitions{0};
  uint32_t policy_id{0};
  bool transpose{false};
};

struct KATANA_EXPORT RDGStat {
  uint64_t num_partitions{0};
  uint32_t policy_id{0};
  bool transpose{false};
};

/// Get Information about the graph
KATANA_EXPORT katana::Result<RDGStat> Stat(const std::string& rdg_name);

KATANA_EXPORT katana::Result<std::vector<RDGView>> ListAvailableViewsForVersion(
    const std::string& rdg_dir, katana::RDGVersion version,
    katana::RDGVersion* max_version);

KATANA_EXPORT katana::Result<std::vector<RDGView>> ListAvailableViews(
    const std::string& rdg_dir);

// Setup and tear down
KATANA_EXPORT katana::Result<void> Init(katana::CommBackend* comm);
KATANA_EXPORT katana::Result<void> Init();

KATANA_EXPORT katana::Result<void> Fini();

}  // namespace tsuba

#endif
