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

struct RDGHandle;

struct RDG {
  // arrow lib returns shared_ptr's to tables; match that for now
  std::shared_ptr<arrow::Table> node_table;
  std::shared_ptr<arrow::Table> edge_table;

  FileView topology_file_storage;

  std::shared_ptr<RDGHandle> handle;

  galois::Result<void>
  AddNodeProperties(const std::shared_ptr<arrow::Table>& table);

  galois::Result<void>
  AddEdgeProperties(const std::shared_ptr<arrow::Table>& table);

  RDG();
};

// acceptable values for Open's flags
constexpr int kReadOnly  = 0;
constexpr int kReadWrite = 1;
/// Open an RDGHandle object from storage
galois::Result<std::shared_ptr<RDGHandle>> Open(const std::string& rdg_name,
                                                int flags);

galois::Result<void> Create(const std::string& name);

// acceptable values for Rename's flags
constexpr int kOverwrite = 1;
// Rename
galois::Result<void> Rename(std::shared_ptr<RDGHandle> handle,
                            const std::string& name, int flags);

/// Load the RDG described by the metadata in @handle into memory
galois::Result<RDG> Load(std::shared_ptr<RDGHandle> handle);
/// Load the RDG described by the metadata in @handle into memory, but only
///    populate the listed properties
galois::Result<RDG> Load(std::shared_ptr<RDGHandle> handle,
                         const std::vector<std::string>& node_properties,
                         const std::vector<std::string>& edge_properties);

galois::Result<void> Store(const RDG& rdg);
galois::Result<void> Store(const RDG& rdg, std::shared_ptr<FileFrame> ff);

galois::Result<void>
AddNodeProperties(RDG* rdg, const std::shared_ptr<arrow::Table>& table);
galois::Result<void>
AddEdgeProperties(RDG* rdg, const std::shared_ptr<arrow::Table>& table);

galois::Result<void> DropNodeProperty(RDG* rdg, int i);
galois::Result<void> DropEdgeProperty(RDG* rdg, int i);

} // namespace tsuba

#endif
