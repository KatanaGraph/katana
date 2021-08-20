#ifndef KATANA_LIBTSUBA_TSUBA_TSUBA_H_
#define KATANA_LIBTSUBA_TSUBA_TSUBA_H_

#include <iterator>
#include <memory>
#include <string>

#include "katana/CommBackend.h"
#include "katana/EntityTypeManager.h"
#include "katana/Iterators.h"
#include "katana/Result.h"
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

  RDGFile(RDGFile&& f) noexcept : handle_(f.handle_) {
    f.handle_ = RDGHandle{nullptr};
  }

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

/// Generate a new canonically named node_entity_type_id file name in the
/// directory associated with handle. Exported to support
/// out-of-core conversion
KATANA_EXPORT katana::Uri MakeNodeEntityTypeIDArrayFileName(RDGHandle handle);

/// Generate a new canonically named edge_entity_type_id file name in the
/// directory associated with handle. Exported to support
/// out-of-core conversion
KATANA_EXPORT katana::Uri MakeEdgeEntityTypeIDArrayFileName(RDGHandle handle);

/// Get the storage directory associated with this handle
KATANA_EXPORT katana::Uri GetRDGDir(RDGHandle handle);

/// Close an RDGHandle object
KATANA_EXPORT katana::Result<void> Close(RDGHandle handle);

/// Create an RDG storage location
/// \param name is storage location prefix that will be used to store the RDG
KATANA_EXPORT katana::Result<void> Create(const std::string& name);

/// @brief Describes properties of RDGView
/// The RDGView will describe will identify the view-type, the arguments used to
/// create it, where it is stored, and the properties of the partioning strategy
/// used to distribute its data across the hosts which will load it.
struct KATANA_EXPORT RDGView {
  uint64_t view_version{0};
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

KATANA_EXPORT katana::Result<std::vector<RDGView>> ListAvailableViews(
    const std::string& rdg_dir);

// Setup and tear down
KATANA_EXPORT katana::Result<void> Init(katana::CommBackend* comm);
KATANA_EXPORT katana::Result<void> Init();

KATANA_EXPORT katana::Result<void> Fini();

/// A set of EntityTypeIDs for use in storage
using StorageSetOfEntityTypeIDs = std::vector<katana::EntityTypeID>;

/// A map from EntityTypeID to a set of EntityTypeIDs
using EntityTypeIDToSetOfEntityTypeIDsStorageMap =
    std::unordered_map<katana::EntityTypeID, StorageSetOfEntityTypeIDs>;

}  // namespace tsuba

#endif
