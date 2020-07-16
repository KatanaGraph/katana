#ifndef GALOIS_LIBTSUBA_TSUBA_RDG_H_
#define GALOIS_LIBTSUBA_TSUBA_RDG_H_

#include <string>
#include <memory>
#include <cstdint>

#include <arrow/api.h>

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
class RDGFile {
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

struct RDG {
  // arrow lib returns shared_ptr's to tables; match that for now
  std::shared_ptr<arrow::Table> node_table;
  std::shared_ptr<arrow::Table> edge_table;

  std::vector<tsuba::PropertyMetadata> node_properties;
  std::vector<tsuba::PropertyMetadata> edge_properties;
  std::vector<std::pair<std::string, std::string>> other_metadata;

  std::string topology_path;
  FileView topology_file_storage;

  /// name of the graph that was used to load this RDG
  std::string rdg_dir;

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
  const GRPrefix* prefix{nullptr};
};

galois::Result<RDGPrefix> ExaminePrefix(RDGHandle handle);

// acceptable values for Open's flags
constexpr int kReadOnly    = 0;
constexpr int kReadWrite   = 1;
constexpr int kReadPartial = 2;
galois::Result<RDGHandle> Open(const std::string& rdg_name, uint32_t flags);

/// Close an RDGHandle object
galois::Result<void> Close(RDGHandle handle);

galois::Result<void> Create(const std::string& name);

// acceptable values for Rename's flags
constexpr int kOverwrite = 1;
// Rename
galois::Result<void> Rename(RDGHandle handle, const std::string& name,
                            int flags);

/// Load the RDG described by the metadata in handle into memory
galois::Result<RDG> Load(RDGHandle handle);

/// Load a contiguous piece of an RDG described by the metadata in the handle
/// into memory
galois::Result<RDG> LoadPartial(RDGHandle handle,
                                std::pair<uint64_t, uint64_t> node_range,
                                std::pair<uint64_t, uint64_t> edge_range,
                                uint64_t topo_off, uint64_t topo_size);

/// Load the RDG described by the metadata in handle into memory, but only
///    populate the listed properties
galois::Result<RDG> Load(RDGHandle handle,
                         const std::vector<std::string>& node_properties,
                         const std::vector<std::string>& edge_properties);
/// Load a contiguous piece of an RDG described by the metadata in the handle
/// into memory
galois::Result<RDG>
LoadPartial(RDGHandle handle, std::pair<uint64_t, uint64_t> node_range,
            std::pair<uint64_t, uint64_t> edge_range, uint64_t topo_off,
            uint64_t topo_size, const std::vector<std::string>& node_properties,
            const std::vector<std::string>& edge_properties);

galois::Result<void> Store(RDGHandle handle, RDG* rdg);
galois::Result<void> Store(RDGHandle handle, RDG* rdg, FileFrame* ff);

galois::Result<void>
AddNodeProperties(RDG* rdg, const std::shared_ptr<arrow::Table>& table);
galois::Result<void>
AddEdgeProperties(RDG* rdg, const std::shared_ptr<arrow::Table>& table);

galois::Result<void> DropNodeProperty(RDG* rdg, int i);
galois::Result<void> DropEdgeProperty(RDG* rdg, int i);

} // namespace tsuba

#endif
