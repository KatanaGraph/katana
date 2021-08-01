#ifndef KATANA_LIBTSUBA_RDGCORE_H_
#define KATANA_LIBTSUBA_RDGCORE_H_

#include <memory>

#include <arrow/api.h>

#include "RDGPartHeader.h"
#include "katana/config.h"
#include "tsuba/FileView.h"

namespace tsuba {

class KATANA_EXPORT RDGCore {
public:
  RDGCore() { InitEmptyProperties(); }

  RDGCore(RDGPartHeader&& part_header) : part_header_(std::move(part_header)) {
    InitEmptyProperties();
  }

  bool Equals(const RDGCore& other) const;

  katana::Result<void> AddNodeProperties(
      const std::shared_ptr<arrow::Table>& props);

  katana::Result<void> AddEdgeProperties(
      const std::shared_ptr<arrow::Table>& props);

  katana::Result<void> UpsertNodeProperties(
      const std::shared_ptr<arrow::Table>& props);

  katana::Result<void> UpsertEdgeProperties(
      const std::shared_ptr<arrow::Table>& props);

  katana::Result<void> RemoveNodeProperty(int i);

  katana::Result<void> RemoveEdgeProperty(int i);

  //
  // Accessors and Mutators
  //

  const std::shared_ptr<arrow::Table>& node_properties() const {
    return node_properties_;
  }
  void set_node_properties(std::shared_ptr<arrow::Table>&& node_properties) {
    node_properties_ = std::move(node_properties);
  }

  const std::shared_ptr<arrow::Table>& edge_properties() const {
    return edge_properties_;
  }
  void set_edge_properties(std::shared_ptr<arrow::Table>&& edge_properties) {
    edge_properties_ = std::move(edge_properties);
  }

  void drop_node_properties() {
    std::vector<std::shared_ptr<arrow::Array>> empty;
    node_properties_ = arrow::Table::Make(arrow::schema({}), empty, 0);
  }
  void drop_edge_properties() {
    std::vector<std::shared_ptr<arrow::Array>> empty;
    edge_properties_ = arrow::Table::Make(arrow::schema({}), empty, 0);
  }

  const FileView& topology_file_storage() const {
    return topology_file_storage_;
  }
  FileView& topology_file_storage() { return topology_file_storage_; }
  void set_topology_file_storage(FileView&& topology_file_storage) {
    topology_file_storage_ = std::move(topology_file_storage);
  }

  const RDGPartHeader& part_header() const { return part_header_; }
  RDGPartHeader& part_header() { return part_header_; }
  void set_part_header(RDGPartHeader&& part_header) {
    part_header_ = std::move(part_header);
  }

  katana::Result<void> RegisterTopologyFile(
      const std::string& branch, const std::string& new_top) {
    part_header_.set_topology_path(branch, new_top);
    return topology_file_storage_.Unbind();
  }

private:
  void InitEmptyProperties();

  //
  // Data
  //

  std::shared_ptr<arrow::Table> node_properties_;
  std::shared_ptr<arrow::Table> edge_properties_;

  FileView topology_file_storage_;

  RDGPartHeader part_header_;
};

}  // namespace tsuba

#endif
