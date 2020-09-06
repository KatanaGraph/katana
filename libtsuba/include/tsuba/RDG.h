#ifndef GALOIS_LIBTSUBA_TSUBA_RDG_H_
#define GALOIS_LIBTSUBA_TSUBA_RDG_H_

#include <string>
#include <memory>
#include <cstdint>

#include <arrow/api.h>
#include <nlohmann/json.hpp>

#include "galois/config.h"
#include "galois/Result.h"
#include "tsuba/FileView.h"
#include "tsuba/FileFrame.h"

namespace tsuba {

struct RDGHandleImpl;

/// RDGHandle is an opaque indentifier for an RDG.
struct RDGHandle {
  RDGHandleImpl* impl_;
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

struct PropertyMetadata {
  std::string name;
  std::string path;
};

struct GALOIS_EXPORT RDGStat {
  uint64_t num_hosts{0};
  uint32_t policy_id{0};
  bool transpose{false};
};

// Struct version of main graph metadatafile
struct GALOIS_EXPORT RDGMeta {
  uint64_t version{0};
  uint64_t previous_version{0};
  uint32_t num_hosts{0};
  uint32_t policy_id{0};
  bool transpose{false};

  // Create an RDGMeta from the named RDG file
  static galois::Result<RDGMeta> Make(const std::string& rdg_name);
  // Canonical naming
  static std::string FileName(const std::string& rdg_path, uint64_t version);
  static std::string PartitionFileName(const std::string& rdg_path,
                                       uint32_t node_id, uint64_t version);

  // Required by nlohmann
  friend void to_json(nlohmann::json& j, const RDGMeta& meta);
  friend void from_json(const nlohmann::json& j, RDGMeta& meta);
};

struct GALOIS_EXPORT PartitionMetadata {
  uint32_t policy_id_;
  bool transposed_;
  bool is_outgoing_edge_cut_;
  bool is_incoming_edge_cut_;
  uint64_t num_global_nodes_;
  uint64_t num_global_edges_;
  uint64_t num_edges_;
  uint32_t num_nodes_;
  uint32_t num_owned_;
  uint32_t num_nodes_with_edges_;
  std::pair<uint32_t, uint32_t> cartesian_grid_;

  std::vector<std::shared_ptr<arrow::ChunkedArray>> mirror_nodes_;
  std::vector<std::shared_ptr<arrow::ChunkedArray>> master_nodes_;
  std::shared_ptr<arrow::ChunkedArray> local_to_global_vector_;
  std::shared_ptr<arrow::ChunkedArray> global_to_local_keys_;
  std::shared_ptr<arrow::ChunkedArray> global_to_local_values_;
};

struct GALOIS_EXPORT RDG {
  // arrow lib returns shared_ptr's to tables; match that for now
  std::shared_ptr<arrow::Table> node_table;
  std::shared_ptr<arrow::Table> edge_table;

  std::vector<tsuba::PropertyMetadata> node_properties;
  std::vector<tsuba::PropertyMetadata> edge_properties;
  std::vector<tsuba::PropertyMetadata> part_properties;
  std::vector<std::pair<std::string, std::string>> other_metadata;

  std::string topology_path;
  uint64_t topology_size;
  FileView topology_file_storage;

  /// name of the graph that was used to load this RDG
  std::string rdg_dir;

  /// Metadata filled in by CuSP
  std::unique_ptr<PartitionMetadata> part_metadata;

  RDG();

  /// Perform some checks on assumed invariants
  galois::Result<void> Validate() const;

  /// Determine if two RDGs are Equal
  bool Equals(const RDG& other) const;
};

struct GRHeader {
  uint64_t version;
  uint64_t edge_type_size;
  uint64_t num_nodes;
  uint64_t num_edges;
};

/// includes the header and the list of indexes
struct GRPrefix {
  tsuba::GRHeader header;
  uint64_t out_indexes[]; /* NOLINT length is defined by num_nodes_ in header */
};

struct RDGPrefix {
  FileView prefix_storage;
  uint64_t view_offset;
  const GRPrefix* prefix{nullptr};
};

GALOIS_EXPORT galois::Result<RDGPrefix> ExaminePrefix(RDGHandle handle);

// acceptable values for Open's flags
constexpr int kReadOnly    = 0;
constexpr int kReadWrite   = 1;
constexpr int kReadPartial = 2;
GALOIS_EXPORT galois::Result<RDGHandle> Open(const std::string& rdg_name,
                                             uint32_t flags);

/// Close an RDGHandle object
GALOIS_EXPORT galois::Result<void> Close(RDGHandle handle);

GALOIS_EXPORT galois::Result<void> Create(const std::string& name);

// acceptable values for Rename's flags
constexpr int kOverwrite = 1;
// Rename
galois::Result<void> Rename(RDGHandle handle, const std::string& name,
                            int flags);

/// Get Information about the graph
GALOIS_EXPORT galois::Result<RDGStat> Stat(const std::string& rdg_name);

// Return all file names that store data for this handle
GALOIS_EXPORT galois::Result<std::unordered_set<std::string>>
FileNames(RDGHandle handle);

/// Load the RDG described by the metadata in handle into memory
GALOIS_EXPORT galois::Result<RDG> Load(RDGHandle handle);

/// Load a contiguous piece of an RDG described by the metadata in the handle
/// into memory
GALOIS_EXPORT galois::Result<RDG>
LoadPartial(RDGHandle handle, std::pair<uint64_t, uint64_t> node_range,
            std::pair<uint64_t, uint64_t> edge_range, uint64_t topo_off,
            uint64_t topo_size);

/// Load the RDG described by the metadata in handle into memory, but only
///    populate the listed properties
GALOIS_EXPORT galois::Result<RDG>
Load(RDGHandle handle, const std::vector<std::string>& node_properties,
     const std::vector<std::string>& edge_properties);
/// Load a contiguous piece of an RDG described by the metadata in the handle
/// into memory
GALOIS_EXPORT galois::Result<RDG>
LoadPartial(RDGHandle handle, std::pair<uint64_t, uint64_t> node_range,
            std::pair<uint64_t, uint64_t> edge_range, uint64_t topo_off,
            uint64_t topo_size, const std::vector<std::string>& node_properties,
            const std::vector<std::string>& edge_properties);

GALOIS_EXPORT galois::Result<void> Store(RDGHandle handle, RDG* rdg);
GALOIS_EXPORT galois::Result<void> Store(RDGHandle handle, RDG* rdg,
                                         FileFrame* ff);

GALOIS_EXPORT galois::Result<void>
AddNodeProperties(RDG* rdg, const std::shared_ptr<arrow::Table>& table);
GALOIS_EXPORT galois::Result<void>
AddEdgeProperties(RDG* rdg, const std::shared_ptr<arrow::Table>& table);

GALOIS_EXPORT galois::Result<void> DropNodeProperty(RDG* rdg, int i);
GALOIS_EXPORT galois::Result<void> DropEdgeProperty(RDG* rdg, int i);

} // namespace tsuba

#endif
