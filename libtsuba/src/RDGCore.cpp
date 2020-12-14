#include "tsuba/RDGCore.h"

#include "tsuba/Errors.h"

namespace {

galois::Result<void>
AddProperties(
    const std::shared_ptr<arrow::Table>& table,
    std::shared_ptr<arrow::Table>* to_update) {
  std::shared_ptr<arrow::Table> current = *to_update;

  if (current->num_columns() > 0 && current->num_rows() != table->num_rows()) {
    GALOIS_LOG_DEBUG(
        "expected {} rows found {} instead", current->num_rows(),
        table->num_rows());
    return tsuba::ErrorCode::InvalidArgument;
  }

  std::shared_ptr<arrow::Table> next = current;

  if (current->num_columns() == 0 && current->num_rows() == 0) {
    next = table;
  } else {
    const auto& schema = table->schema();
    int last = current->num_columns();

    for (int i = 0, n = schema->num_fields(); i < n; i++) {
      auto result =
          next->AddColumn(last + i, schema->field(i), table->column(i));
      if (!result.ok()) {
        GALOIS_LOG_DEBUG("arrow error: {}", result.status());
        return tsuba::ErrorCode::ArrowError;
      }

      next = result.ValueOrDie();
    }
  }

  if (!next->schema()->HasDistinctFieldNames()) {
    GALOIS_LOG_DEBUG("failed: column names are not distinct");
    return tsuba::ErrorCode::Exists;
  }

  *to_update = next;

  return galois::ResultSuccess();
}

}  // namespace

namespace tsuba {

galois::Result<void>
RDGCore::AddNodeProperties(const std::shared_ptr<arrow::Table>& table) {
  return AddProperties(table, &node_table_);
}

galois::Result<void>
RDGCore::AddEdgeProperties(const std::shared_ptr<arrow::Table>& table) {
  return AddProperties(table, &edge_table_);
}

void
RDGCore::InitEmptyTables() {
  std::vector<std::shared_ptr<arrow::Array>> empty;
  node_table_ = arrow::Table::Make(arrow::schema({}), empty, 0);
  edge_table_ = arrow::Table::Make(arrow::schema({}), empty, 0);
}

bool
RDGCore::Equals(const RDGCore& other) const {
  // Assumption: t_f_s and other.t_f_s are both fully loaded into memory
  return topology_file_storage_.size() == other.topology_file_storage_.size() &&
         !memcmp(
             topology_file_storage_.ptr<uint8_t>(),
             other.topology_file_storage_.ptr<uint8_t>(),
             topology_file_storage_.size()) &&
         node_table_->Equals(*other.node_table_, true) &&
         edge_table_->Equals(*other.edge_table_, true);
}

galois::Result<void>
RDGCore::DropNodeProperty(uint32_t i) {
  auto result = node_table_->RemoveColumn(i);
  if (!result.ok()) {
    GALOIS_LOG_DEBUG("arrow error: {}", result.status());
    return ErrorCode::ArrowError;
  }

  node_table_ = std::move(result.ValueOrDie());

  part_header_.DropNodeProperty(i);

  return galois::ResultSuccess();
}

galois::Result<void>
RDGCore::DropEdgeProperty(uint32_t i) {
  auto result = edge_table_->RemoveColumn(i);
  if (!result.ok()) {
    GALOIS_LOG_DEBUG("arrow error: {}", result.status());
    return ErrorCode::ArrowError;
  }

  edge_table_ = std::move(result.ValueOrDie());

  part_header_.DropEdgeProperty(i);

  return galois::ResultSuccess();
}

}  // namespace tsuba
