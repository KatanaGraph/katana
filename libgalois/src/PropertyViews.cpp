#include <galois/graphs/PropertyViews.h>

galois::Result<std::vector<arrow::Array*>>
galois::graphs::internal::ExtractArrays(
    const arrow::Table* table, const std::vector<std::string>& properties) {
  std::vector<arrow::Array*> ret;
  for (auto& property : properties) {
    auto column = table->GetColumnByName(property);
    if (!column) {
      return ErrorCode::PropertyNotFound;
    }
    if (column->num_chunks() != 1) {
      // Katana form graphs only contain single chunk property columns.
      return ErrorCode::TODO;
      // TODO: Maybe we need an InvalidGraph error
    }
    ret.emplace_back(column->chunks()[0].get());
  }

  return ret;
}
