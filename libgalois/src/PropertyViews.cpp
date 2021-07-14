#include <katana/PropertyViews.h>

katana::Result<std::vector<arrow::Array*>>
katana::internal::ExtractArrays(
    const arrow::Table* table, const std::vector<std::string>& properties) {
  std::vector<arrow::Array*> ret;
  for (auto& property : properties) {
    auto column = table->GetColumnByName(property);
    if (!column) {
      return ErrorCode::PropertyNotFound;
    }
    if (column->num_chunks() != 1) {
      // Katana form graphs only contain single chunk property columns.
      return KATANA_ERROR(
          ErrorCode::NotImplemented, "property is in the wrong format");
    }
    ret.emplace_back(column->chunks()[0].get());
  }

  return ret;
}

katana::Result<std::vector<arrow::Array*>>
katana::internal::ExtractArrays(
    const PropertyGraph::ReadOnlyPropertyView& pview,
    const std::vector<std::string>& properties) {
  std::vector<arrow::Array*> ret;
  for (auto& property : properties) {
    auto column = pview.GetProperty(property);
    if (!column) {
      return ErrorCode::PropertyNotFound;
    }
    if (column->num_chunks() != 1) {
      // Katana form graphs only contain single chunk property columns.
      return KATANA_ERROR(
          ErrorCode::NotImplemented, "property is in the wrong format");
    }
    ret.emplace_back(column->chunks()[0].get());
  }

  return ret;
}
