#include "Transforms.h"

#include <arrow/type.h>

#include "TimeParser.h"

namespace {

void
ApplyTransform(
    katana::PropertyGraph::MutablePropertyView view,
    katana::ColumnTransformer* transform, tsuba::TxnContext* txn_ctx) {
  int cur_field = 0;
  int num_fields = view.loaded_schema()->num_fields();
  std::vector<std::shared_ptr<arrow::Field>> new_fields;
  std::vector<std::shared_ptr<arrow::ChunkedArray>> new_columns;

  while (cur_field < num_fields) {
    auto field = view.loaded_schema()->field(cur_field);
    if (!transform->Matches(field.get())) {
      ++cur_field;
      continue;
    }

    KATANA_LOG_WARN(
        "applying {} to property {}", transform->name(), field->name());

    std::shared_ptr<arrow::ChunkedArray> property = view.GetProperty(cur_field);

    if (auto result = view.RemoveProperty(cur_field, txn_ctx); !result) {
      KATANA_LOG_FATAL("failed to remove {}: {}", cur_field, result.error());
    }

    // Reread num_fields from view.schema rather than caching schema() value
    // because RemoveProperty may have updated view itself.
    num_fields = view.loaded_schema()->num_fields();

    auto [new_field, new_column] = (*transform)(field.get(), property.get());

    new_fields.emplace_back(new_field);
    new_columns.emplace_back(new_column);
  }

  if (!new_fields.empty()) {
    auto schema = std::make_shared<arrow::Schema>(new_fields);
    std::shared_ptr<arrow::Table> new_table =
        arrow::Table::Make(schema, new_columns);
    if (auto result = view.AddProperties(new_table, txn_ctx); !result) {
      KATANA_LOG_FATAL("failed to add properties: {}", result.error());
    }
  }
}

}  // namespace

std::pair<std::shared_ptr<arrow::Field>, std::shared_ptr<arrow::ChunkedArray>>
katana::ConvertDateTime::operator()(
    arrow::Field* field, arrow::ChunkedArray* chunked_array) {
  std::vector<int64_t> values;
  std::vector<bool> is_valid;

  values.reserve(chunked_array->length());
  is_valid.reserve(chunked_array->length());

  std::unique_ptr<arrow::ArrayBuilder> builder;
  if (auto st =
          arrow::MakeBuilder(arrow::default_memory_pool(), dtype_, &builder);
      !st.ok()) {
    KATANA_LOG_FATAL("failed to create builder");
  }

  for (int cidx = 0, num_chunks = chunked_array->num_chunks();
       cidx < num_chunks; ++cidx) {
    std::shared_ptr<arrow::Array> chunk = chunked_array->chunk(cidx);
    auto array = std::dynamic_pointer_cast<arrow::StringArray>(chunk);

    if (!array) {
      KATANA_LOG_FATAL("column not string");
    }

    switch (dtype_->id()) {
    case arrow::Type::TIMESTAMP: {
      TimeParser<arrow::TimestampType, std::chrono::nanoseconds> parser;
      parser.ParseInto(*array, builder.get());
      break;
    }
    case arrow::Type::DATE32: {
      TimeParser<arrow::Date32Type, date::days> parser;
      parser.ParseInto(*array, builder.get());
      break;
    }
    case arrow::Type::DATE64: {
      TimeParser<arrow::Date64Type, std::chrono::milliseconds> parser;
      parser.ParseInto(*array, builder.get());
      break;
    }
    default:
      KATANA_LOG_FATAL("unsupported type: ({})", dtype_->ToString());
    }
  }

  auto new_field = field->WithType(dtype_)->WithNullable(true);

  std::shared_ptr<arrow::Array> new_array;
  if (auto st = builder->Finish(&new_array); !st.ok()) {
    KATANA_LOG_FATAL("could not finish array: {}", st);
  }

  auto new_column = std::make_shared<arrow::ChunkedArray>(
      std::vector<std::shared_ptr<arrow::Array>>{std::move(new_array)});

  return std::make_pair(new_field, new_column);
}

std::pair<std::shared_ptr<arrow::Field>, std::shared_ptr<arrow::ChunkedArray>>
katana::SparsifyBooleans::operator()(
    arrow::Field* field, arrow::ChunkedArray* chunked_array) {
  std::vector<uint8_t> values;

  values.reserve(chunked_array->length());

  for (int cidx = 0, num_chunks = chunked_array->num_chunks();
       cidx < num_chunks; ++cidx) {
    std::shared_ptr<arrow::Array> chunk = chunked_array->chunk(cidx);
    auto array = std::dynamic_pointer_cast<arrow::BooleanArray>(chunk);

    if (!array) {
      KATANA_LOG_FATAL("column not boolean");
    }

    for (int64_t i = 0, n = array->length(); i < n; ++i) {
      uint8_t value = array->IsValid(i) && array->Value(i) ? 1 : 0;
      values.emplace_back(value);
    }
  }

  arrow::UInt8Builder builder;
  if (auto result = builder.AppendValues(values); !result.ok()) {
    KATANA_LOG_FATAL("could not append array: {}", result);
  }

  std::shared_ptr<arrow::Array> new_array;
  if (auto result = builder.Finish(&new_array); !result.ok()) {
    KATANA_LOG_FATAL("could not finish array: {}", result);
  }

  auto new_field = field->WithType(arrow::uint8())->WithNullable(false);
  auto new_column = std::make_shared<arrow::ChunkedArray>(
      std::vector<std::shared_ptr<arrow::Array>>{new_array});

  return std::make_pair(new_field, new_column);
}

void
katana::ApplyTransforms(
    katana::PropertyGraph* graph,
    const std::vector<std::unique_ptr<katana::ColumnTransformer>>& transformers,
    tsuba::TxnContext* txn_ctx) {
  for (const auto& t : transformers) {
    ApplyTransform(graph->NodeMutablePropertyView(), t.get(), txn_ctx);
    ApplyTransform(graph->EdgeMutablePropertyView(), t.get(), txn_ctx);
  }
}
