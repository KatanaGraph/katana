#include "Transforms.h"

#include <arrow/type.h>

#include "galois/Logging.h"
#include "TimeParser.h"

namespace {

void ApplyTransform(galois::graphs::PropertyFileGraph::PropertyView view,
                    galois::ColumnTransformer* transform) {
  int cur_field  = 0;
  int num_fields = view.schema()->num_fields();
  std::vector<std::shared_ptr<arrow::Field>> new_fields;
  std::vector<std::shared_ptr<arrow::ChunkedArray>> new_columns;

  while (cur_field < num_fields) {
    auto field = view.schema()->field(cur_field);
    if (!transform->Matches(field.get())) {
      ++cur_field;
      continue;
    }

    GALOIS_LOG_WARN("applying {} to property {}", transform->name(),
                    field->name());

    std::shared_ptr<arrow::ChunkedArray> property = view.Property(cur_field);

    if (auto result = view.RemoveProperty(cur_field); !result) {
      GALOIS_LOG_FATAL("failed to remove {}: {}", cur_field, result.error());
    }

    // Reread num_fields from view.schema rather than caching schema() value
    // because RemoveProperty may have updated view itself.
    num_fields = view.schema()->num_fields();

    auto [new_field, new_column] = (*transform)(field.get(), property.get());

    new_fields.emplace_back(new_field);
    new_columns.emplace_back(new_column);
  }

  if (!new_fields.empty()) {
    auto schema = std::make_shared<arrow::Schema>(new_fields);
    std::shared_ptr<arrow::Table> new_table =
        arrow::Table::Make(schema, new_columns);
    if (auto result = view.AddProperties(new_table); !result) {
      GALOIS_LOG_FATAL("failed to add properties: {}", result.error());
    }
  }
}

} // namespace

std::pair<std::shared_ptr<arrow::Field>, std::shared_ptr<arrow::ChunkedArray>>
galois::ConvertTimestamps::operator()(arrow::Field* field,
                                      arrow::ChunkedArray* chunked_array) {
  std::vector<int64_t> values;
  std::vector<bool> is_valid;

  values.reserve(chunked_array->length());
  is_valid.reserve(chunked_array->length());

  TimeParser<std::chrono::nanoseconds> parser;

  for (int cidx = 0, num_chunks = chunked_array->num_chunks();
       cidx < num_chunks; ++cidx) {
    std::shared_ptr<arrow::Array> chunk = chunked_array->chunk(cidx);
    auto array = std::dynamic_pointer_cast<arrow::StringArray>(chunk);

    if (!array) {
      GALOIS_LOG_FATAL("column not string");
    }

    for (int64_t i = 0, n = array->length(); i < n; ++i) {
      int64_t ts = 0;
      bool valid = array->IsValid(i);

      if (valid) {
        std::string str = array->GetString(i);
        auto r          = parser.Parse(str);
        if (r) {
          valid = true;
          ts    = *r;
        } else {
          GALOIS_LOG_WARN("could not parse datetime string {}", str);
          valid = false;
        }
      }

      values.emplace_back(ts);
      is_valid.emplace_back(valid);
    }
  }

  // Technically, a Unix timestamp is not in UTC because it does not account
  // for leap seconds since the beginning of the epoch. Parquet and arrow use
  // Unix timestamps throughout so they also avoid accounting for this
  // distinction.
  auto timestamp_type =
      std::make_shared<arrow::TimestampType>(arrow::TimeUnit::NANO, "UTC");

  auto new_field = field->WithType(timestamp_type)->WithNullable(true);

  arrow::TimestampBuilder builder(new_field->type(),
                                  arrow::default_memory_pool());
  if (auto result = builder.AppendValues(values, is_valid); !result.ok()) {
    GALOIS_LOG_FATAL("could not append array: {}", result);
  }

  std::shared_ptr<arrow::Array> new_array;
  if (auto result = builder.Finish(&new_array); !result.ok()) {
    GALOIS_LOG_FATAL("could not finish array: {}", result);
  }

  auto new_column = std::make_shared<arrow::ChunkedArray>(
      std::vector<std::shared_ptr<arrow::Array>>{new_array});

  return std::make_pair(new_field, new_column);
}

std::pair<std::shared_ptr<arrow::Field>, std::shared_ptr<arrow::ChunkedArray>>
galois::SparsifyBooleans::operator()(arrow::Field* field,
                                     arrow::ChunkedArray* chunked_array) {
  std::vector<uint8_t> values;

  values.reserve(chunked_array->length());

  for (int cidx = 0, num_chunks = chunked_array->num_chunks();
       cidx < num_chunks; ++cidx) {
    std::shared_ptr<arrow::Array> chunk = chunked_array->chunk(cidx);
    auto array = std::dynamic_pointer_cast<arrow::BooleanArray>(chunk);

    if (!array) {
      GALOIS_LOG_FATAL("column not boolean");
    }

    for (int64_t i = 0, n = array->length(); i < n; ++i) {
      uint8_t value = array->IsValid(i) && array->Value(i) ? 1 : 0;
      values.emplace_back(value);
    }
  }

  arrow::UInt8Builder builder;
  if (auto result = builder.AppendValues(values); !result.ok()) {
    GALOIS_LOG_FATAL("could not append array: {}", result);
  }

  std::shared_ptr<arrow::Array> new_array;
  if (auto result = builder.Finish(&new_array); !result.ok()) {
    GALOIS_LOG_FATAL("could not finish array: {}", result);
  }

  auto new_field  = field->WithType(arrow::uint8())->WithNullable(false);
  auto new_column = std::make_shared<arrow::ChunkedArray>(
      std::vector<std::shared_ptr<arrow::Array>>{new_array});

  return std::make_pair(new_field, new_column);
}

void galois::ApplyTransforms(
    galois::graphs::PropertyFileGraph* graph,
    const std::vector<std::unique_ptr<galois::ColumnTransformer>>&
        transformers) {
  for (const auto& t : transformers) {
    ApplyTransform(graph->node_property_view(), t.get());
    ApplyTransform(graph->edge_property_view(), t.get());
  }
}
