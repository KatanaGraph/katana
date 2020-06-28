#ifndef GALOIS_LIBTSUBA_TSUBA_RDG_H_
#define GALOIS_LIBTSUBA_TSUBA_RDG_H_

#include <arrow/api.h>

#include <memory>

#include "galois/Result.h"
#include "tsuba/FileView.h"

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

} // namespace tsuba

#endif
