#ifndef KATANA_LIBTSUBA_KATANA_TSUBA_H_
#define KATANA_LIBTSUBA_KATANA_TSUBA_H_

#include <iterator>
#include <memory>
#include <string>

#include "katana/CommBackend.h"
#include "katana/EntityTypeManager.h"
#include "katana/Iterators.h"
#include "katana/Result.h"
#include "katana/URI.h"
#include "katana/config.h"

namespace katana {

class RDGHandleImpl;
class RDGManifest;
class TxnContext;

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

KATANA_EXPORT katana::Result<RDGManifest> FindManifest(
    const std::string& rdg_name);

KATANA_EXPORT katana::Result<katana::RDGManifest> FindManifest(
    const std::string& rdg_name, katana::TxnContext* txn_ctx);

KATANA_EXPORT katana::Result<RDGHandle> Open(
    RDGManifest rdg_manifest, uint32_t flags);

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
  std::string view_type;
  std::string view_args;
  std::string view_path;
  uint64_t num_partitions{0};
  uint32_t policy_id{0};
  bool transpose{false};
};

/// list the views in storage for a particular version of an RDG
/// \param rdg_dir is the RDG's URI prefix
/// \param version is an optional version argument, if omitted this will return
///    the views for the latest version
/// \returns a pair (RDG version, vector of RDGViews) or ErrorCode::NotFound if
/// rdg_dir contains no manifest files
KATANA_EXPORT katana::Result<std::pair<uint64_t, std::vector<RDGView>>>
ListViewsOfVersion(
    const std::string& rdg_dir, std::optional<uint64_t> version = std::nullopt);

/// deprecated; duplicate of ListViewsOfVersion maintained for compatibility
KATANA_EXPORT katana::Result<std::pair<uint64_t, std::vector<RDGView>>>
ListAvailableViews(
    const std::string& rdg_dir, std::optional<uint64_t> version = std::nullopt);

KATANA_EXPORT katana::Result<std::vector<std::pair<katana::Uri, katana::Uri>>>
CreateSrcDestFromViewsForCopy(
    const std::string& src_dir, const std::string& dst_dir, uint64_t version);

/// CopyRDG copies RDG files from a source to a destination.
/// E.g. SRC_DIR/part_vers0003_rdg_node00000 -> DST_DIR/part_vers0001_rdg_node_00000
/// The argument is a list of source and destination pairs as an RDG consists of many files.
/// See CreateSrcDestFromViewsForCopy for how to generate this list from an RDG prefix and version
/// \param src_dst_files is a vector of src-dest pairs for individual RDG files
/// \returns a Result to indicate whether the method succeeded or failed
KATANA_EXPORT katana::Result<void> CopyRDG(
    std::vector<std::pair<katana::Uri, katana::Uri>> src_dst_files);

// Setup and tear down
KATANA_EXPORT katana::Result<void> InitTsuba(katana::CommBackend* comm);
KATANA_EXPORT katana::Result<void> InitTsuba();

KATANA_EXPORT katana::Result<void> FiniTsuba();

/// A set of EntityTypeIDs for use in storage
using StorageSetOfEntityTypeIDs = std::vector<katana::EntityTypeID>;

/// A map from EntityTypeID to a set of EntityTypeIDs
using EntityTypeIDToSetOfEntityTypeIDsStorageMap =
    std::unordered_map<katana::EntityTypeID, StorageSetOfEntityTypeIDs>;

/// Dictactes the max number of RDGTopologies PartitionTopologyMetadataEntries
/// can be increased if required
constexpr size_t kMaxNumTopologies = 64;

/// RDGPropInfo details the property name and its respective path in the RDG dir.
struct RDGPropInfo {
  std::string property_name;
  std::string property_path;
};

// N.B. This is a temporary interface used to write RDG part header given some
// amount of information regarding properties, type manager, etc. The primary consumer
// of this interface is the out-of-core import path, which currently writes out property
// and type information on its own. I do NOT advise that anyone use this method without
// understanding the assumptions it makes.
// TODO (vkarthik): get rid of this interface and have a proper unified one
KATANA_EXPORT katana::Result<void> WriteRDGPartHeader(
    std::vector<katana::RDGPropInfo> node_properties,
    std::vector<katana::RDGPropInfo> edge_properties,
    katana::EntityTypeManager& node_entity_type_manager,
    katana::EntityTypeManager& edge_entity_type_manager,
    const std::string& node_entity_type_id_array_path,
    const std::string& edge_entity_type_id_array_path, uint64_t num_nodes,
    uint64_t num_edges, const std::string& topology_path,
    const std::string& rdg_dir);

// N.B. This is also a temporary interface used to write out RDG manifest files for
// out-of-core CSV import.
// TODO (vkarthik): I think this interface should get removed and manifests should be
// written through the Go server. Need to figure out how to do this properly for
// python operations.
KATANA_EXPORT katana::Result<void> WriteRDGManifest(
    const std::string& rdg_dir, uint32_t num_hosts);

}  // namespace katana

#endif
