#include "RDGCore.h"

#include "RDGPartHeader.h"
#include "tsuba/Errors.h"

namespace {

katana::Result<void>
UpsertProperties(
    const std::shared_ptr<arrow::Table>& props,
    std::shared_ptr<arrow::Table>* to_update) {
  std::shared_ptr<arrow::Table> current = *to_update;

  if (current->num_columns() > 0 && current->num_rows() != props->num_rows()) {
    return KATANA_ERROR(
        tsuba::ErrorCode::InvalidArgument, "expected {} rows found {} instead",
        current->num_rows(), props->num_rows());
  }

  std::shared_ptr<arrow::Table> next = current;

  if (current->num_columns() == 0 && current->num_rows() == 0) {
    next = props;
  } else {
    const auto& schema = props->schema();
    int last = current->num_columns();

    for (int i = 0, n = schema->num_fields(); i < n; i++) {
      auto name = schema->field(i)->name();
      auto current_col = next->GetColumnByName(name);
      arrow::Result<std::shared_ptr<arrow::Table>> result;
      std::string error_context = "insert";
      if (current_col == nullptr) {
        // Insert the column, error_context == "insert"
        result = next->AddColumn(last++, schema->field(i), props->column(i));
      } else {
        // Update the column
        error_context = "update";
        auto col_names = next->ColumnNames();
        // Column names are not sorted, but assumed to be less than 100s
        auto col_it = std::find(col_names.begin(), col_names.end(), name);
        KATANA_LOG_ASSERT(
            col_it != col_names.end());  // GetColumnByName != null
        result = next->SetColumn(
            std::distance(col_names.begin(), col_it), schema->field(i),
            props->column(i));
      }
      if (!result.ok()) {
        return KATANA_ERROR(
            tsuba::ErrorCode::ArrowError, "arrow error {}: {}", error_context,
            result.status());
      }

      next = result.ValueOrDie();
    }
  }

  if (!next->schema()->HasDistinctFieldNames()) {
    return KATANA_ERROR(
        tsuba::ErrorCode::Exists, "column names are not distinct");
  }

  *to_update = next;

  return katana::ResultSuccess();
}

katana::Result<void>
AddProperties(
    const std::shared_ptr<arrow::Table>& props,
    std::shared_ptr<arrow::Table>* to_update) {
  auto col_names = (*to_update)->ColumnNames();

  const auto& schema = props->schema();
  for (int i = 0, n = schema->num_fields(); i < n; i++) {
    auto name = schema->field(i)->name();
    // Column names are not sorted, but assumed to be less than 100s
    auto col_it = std::find(col_names.begin(), col_names.end(), name);
    if (col_it != col_names.end()) {
      return KATANA_ERROR(
          tsuba::ErrorCode::Exists, "column names are not distinct");
    }
  }
  return UpsertProperties(props, to_update);
}

}  // namespace

namespace tsuba {

katana::Result<void>
RDGCore::AddNodeProperties(const std::shared_ptr<arrow::Table>& props) {
  return AddProperties(props, &node_properties_);
}

katana::Result<void>
RDGCore::AddEdgeProperties(const std::shared_ptr<arrow::Table>& props) {
  return AddProperties(props, &edge_properties_);
}

katana::Result<void>
RDGCore::UpsertNodeProperties(const std::shared_ptr<arrow::Table>& props) {
  return UpsertProperties(props, &node_properties_);
}

katana::Result<void>
RDGCore::UpsertEdgeProperties(const std::shared_ptr<arrow::Table>& props) {
  return UpsertProperties(props, &edge_properties_);
}

void
RDGCore::InitEmptyProperties() {
  std::vector<std::shared_ptr<arrow::Array>> empty;
  node_properties_ = arrow::Table::Make(arrow::schema({}), empty, 0);
  edge_properties_ = arrow::Table::Make(arrow::schema({}), empty, 0);
}

bool
RDGCore::Equals(const RDGCore& other) const {
  // Assumption: t_f_s and other.t_f_s are both fully loaded into memory
  return topology_file_storage_.size() == other.topology_file_storage_.size() &&
         !memcmp(
             topology_file_storage_.ptr<uint8_t>(),
             other.topology_file_storage_.ptr<uint8_t>(),
             topology_file_storage_.size()) &&
         node_properties_->Equals(*other.node_properties_, true) &&
         edge_properties_->Equals(*other.edge_properties_, true);
}

katana::Result<void>
RDGCore::RemoveNodeProperty(uint32_t i) {
  auto result = node_properties_->RemoveColumn(i);
  if (!result.ok()) {
    return KATANA_ERROR(
        ErrorCode::ArrowError, "arrow error: {}", result.status());
  }

  node_properties_ = std::move(result.ValueOrDie());

  part_header_.RemoveNodeProperty(i);

  return katana::ResultSuccess();
}

katana::Result<void>
RDGCore::RemoveEdgeProperty(uint32_t i) {
  auto result = edge_properties_->RemoveColumn(i);
  if (!result.ok()) {
    return KATANA_ERROR(
        ErrorCode::ArrowError, "arrow error: {}", result.status());
  }

  edge_properties_ = std::move(result.ValueOrDie());

  part_header_.RemoveEdgeProperty(i);

  return katana::ResultSuccess();
}

}  // namespace tsuba
