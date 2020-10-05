#ifndef GALOIS_LIBTSUBA_TSUBA_RDG_H_
#define GALOIS_LIBTSUBA_TSUBA_RDG_H_

#include <cstdint>
#include <memory>
#include <string>

#include <arrow/api.h>
#include <nlohmann/json.hpp>

#include "galois/Result.h"
#include "galois/Uri.h"
#include "galois/config.h"
#include "tsuba/Errors.h"
#include "tsuba/FileFrame.h"
#include "tsuba/FileView.h"

namespace tsuba {

struct RDGHandleImpl;

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

class RDGLineage {
public:
  std::string command_line_{};
  void AddCommandLine(const std::string& cmd) {
    if (!command_line_.empty()) {
      GALOIS_LOG_DEBUG(
          "Add command line to lineage was: {} is: {}", command_line_, cmd);
    }
    command_line_ = cmd;
  }
  void ClearLineage() { command_line_.clear(); }
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
class GALOIS_EXPORT RDGMeta {
public:
  uint64_t version_{0};
  uint64_t previous_version_{0};
  uint32_t num_hosts_{0};  // 0 is a reserved value for the empty RDG when
  // tsuba views policy_id as zero (not partitioned) or not zero (partitioned
  // according to a CuSP-specific policy)
  uint32_t policy_id_{0};
  bool transpose_{false};
  RDGLineage lineage_;

  galois::Uri dir_;  // not persisted; inferred from name

  RDGMeta(galois::Uri dir) : dir_(std::move(dir)) {}
  RDGMeta(
      uint64_t version, uint64_t previous_version, uint32_t num_hosts,
      uint32_t policy_id, bool transpose, galois::Uri dir,
      const RDGLineage& lineage)
      : version_(version),
        previous_version_(previous_version),
        num_hosts_(num_hosts),
        policy_id_(policy_id),
        transpose_(transpose),
        lineage_(lineage),
        dir_(std::move(dir)) {}
  RDGMeta() = default;

  /// Create an RDGMeta
  /// \param uri a uri that either names a registered RDG or an explicit
  ///    rdg file
  /// \returns the constructed RDGMeta and the directory of its contents
  static galois::Result<RDGMeta> Make(const galois::Uri& uri);

  /// Create an RDGMeta
  /// \param uri is a storage prefix where the RDG is stored
  /// \param version is the version of the meta_dir to load
  /// \returns the constructed RDGMeta and the directory of its contents
  static galois::Result<RDGMeta> Make(const galois::Uri& uri, uint64_t version);

  // Canonical naming
  static galois::Uri FileName(const galois::Uri& uri, uint64_t version);

  static galois::Uri PartitionFileName(
      const galois::Uri& uri, uint32_t node_id, uint64_t version);
  std::string ToJsonString() const;

  static std::string PartitionFileName(uint32_t node_id, uint64_t version);

  // Required by nlohmann
  friend void to_json(nlohmann::json& j, const RDGMeta& meta);
  friend void from_json(const nlohmann::json& j, RDGMeta& meta);
};

struct GALOIS_EXPORT PartitionMetadata {
  enum class State {
    kUninitialized,
    kFromStorage,
    kFromPartitioner,
  };
  State state{State::kUninitialized};
  uint32_t policy_id_{0};
  bool transposed_{false};
  bool is_outgoing_edge_cut_{false};
  bool is_incoming_edge_cut_{false};
  uint64_t num_global_nodes_{0UL};
  uint64_t num_global_edges_{0UL};
  uint64_t num_edges_{0UL};
  uint32_t num_nodes_{0};
  uint32_t num_owned_{0};
  uint32_t num_nodes_with_edges_{0};
  std::pair<uint32_t, uint32_t> cartesian_grid_{0, 0};

  std::vector<std::shared_ptr<arrow::ChunkedArray>> mirror_nodes_;
  std::vector<std::shared_ptr<arrow::ChunkedArray>> master_nodes_;
  std::shared_ptr<arrow::ChunkedArray> local_to_global_vector_{};
  std::shared_ptr<arrow::ChunkedArray> global_to_local_keys_{};
  std::shared_ptr<arrow::ChunkedArray> global_to_local_values_{};
};

class GALOIS_EXPORT RDG {
public:
  struct SliceArg {
    std::pair<uint64_t, uint64_t> node_range;
    std::pair<uint64_t, uint64_t> edge_range;
    uint64_t topo_off;
    uint64_t topo_size;
  };

  // arrow lib returns shared_ptr's to tables; match that for now
  std::shared_ptr<arrow::Table> node_table_;
  std::shared_ptr<arrow::Table> edge_table_;

  std::vector<tsuba::PropertyMetadata> node_properties_;
  std::vector<tsuba::PropertyMetadata> edge_properties_;
  std::vector<tsuba::PropertyMetadata> part_properties_;
  // Deprecated, will go away with parquet-format partition files
  std::vector<std::pair<std::string, std::string>> other_metadata_;
  /// Metadata filled in by CuSP, or from storage (meta partition file)
  PartitionMetadata part_metadata_;

  std::string topology_path_;
  uint64_t topology_size_;
  FileView topology_file_storage_;

  /// name of the graph that was used to load this RDG
  galois::Uri rdg_dir_;
  // How this graph was derived from the previous version
  RDGLineage lineage_;

  RDG();

  /// Perform some checks on assumed invariants
  galois::Result<void> Validate() const;

  /// Determine if two RDGs are Equal
  bool Equals(const RDG& other) const;

  /// Store this RDG at `handle`, if `ff` is not null, it is assumed to contain
  /// an updated topology and persisted as such
  galois::Result<void> Store(
      RDGHandle handle, const std::string& command_line,
      FileFrame* ff = nullptr);

  galois::Result<void> AddNodeProperties(
      const std::shared_ptr<arrow::Table>& table);

  galois::Result<void> AddEdgeProperties(
      const std::shared_ptr<arrow::Table>& table);

  galois::Result<void> DropNodeProperty(int i);
  galois::Result<void> DropEdgeProperty(int i);

  /// Explain to graph how it is derived from previous version
  void AddLineage(const std::string& command_line);

  /// Load the RDG described by the metadata in handle into memory
  static galois::Result<RDG> Load(
      RDGHandle handle, const std::vector<std::string>* node_props = nullptr,
      const std::vector<std::string>* edge_props = nullptr);

  static galois::Result<RDG> Load(
      const std::string& rdg_meta_path,
      const std::vector<std::string>* node_props = nullptr,
      const std::vector<std::string>* edge_props = nullptr);

  /// Load a contiguous piece of an RDG described by the metadata in the handle
  /// into memory
  static galois::Result<RDG> LoadPartial(
      RDGHandle handle, const SliceArg& slice,
      const std::vector<std::string>* node_props = nullptr,
      const std::vector<std::string>* edge_props = nullptr);

  static galois::Result<RDG> LoadPartial(
      const std::string& rdg_meta_path, const SliceArg& slice,
      const std::vector<std::string>* node_props = nullptr,
      const std::vector<std::string>* edge_props = nullptr);

private:
  galois::Result<void> DoLoad(const galois::Uri& metadata_dir);

  galois::Result<void> DoLoadPartial(
      const galois::Uri& metadata_dir, const SliceArg& slice);

  static galois::Result<RDG> Make(
      const galois::Uri& partition_path,
      const std::vector<std::string>* node_props,
      const std::vector<std::string>* edge_props, const SliceArg* slice);

  galois::Result<std::string> MakeMetadataJson() const;
  std::pair<std::vector<std::string>, std::vector<std::string>> MakeMetadata()
      const;

  galois::Result<void> WriteMetadataJson(RDGHandle handle) const;

  galois::Result<void> PrunePropsTo(
      const std::vector<std::string>* node_properties,
      const std::vector<std::string>* edge_properties);

  galois::Result<void> DoStore(
      RDGHandle handle, const std::string& command_line);

  void UnbindFromStorage();
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
GALOIS_EXPORT galois::Result<RDGPrefix> ExaminePrefix(const std::string& uri);

// acceptable values for Open's flags
constexpr int kReadOnly = 0;
constexpr int kReadWrite = 1;
constexpr int kReadPartial = 2;
GALOIS_EXPORT galois::Result<RDGHandle> Open(
    const std::string& rdg_name, uint32_t flags);

/// Close an RDGHandle object
GALOIS_EXPORT galois::Result<void> Close(RDGHandle handle);

/// Create a managed RDG
/// \param name is storage location prefix that will be used to store the RDG
GALOIS_EXPORT galois::Result<void> Create(const std::string& name);

/// Register a previously created RDG attaching it to the namespace; infer the
/// version by examining files in name
/// \param name is storage location prefix that the RDG is stored in
GALOIS_EXPORT galois::Result<void> Register(const std::string& name);

/// Forget an RDG, detaching it from the namespace
/// \param name is storage location prefix that the RDG is stored in
GALOIS_EXPORT galois::Result<void> Forget(const std::string& name);

/// Get Information about the graph
GALOIS_EXPORT galois::Result<RDGStat> Stat(const std::string& rdg_name);

}  // namespace tsuba

#endif
