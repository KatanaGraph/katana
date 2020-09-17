#ifndef GALOIS_TOOLS_GRAPH_CONVERT_TRANSFORMS_H
#define GALOIS_TOOLS_GRAPH_CONVERT_TRANSFORMS_H

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include <arrow/api.h>
#include <arrow/type.h>

#include "galois/graphs/PropertyFileGraph.h"

namespace galois {

/// A ColumnTransformer rewrites a column when matches is true.
class ColumnTransformer {
public:
  virtual ~ColumnTransformer() = default;

  virtual std::pair<
      std::shared_ptr<arrow::Field>, std::shared_ptr<arrow::ChunkedArray>>
  operator()(arrow::Field* field, arrow::ChunkedArray* chunked_array) = 0;

  virtual bool Matches(arrow::Field* field) = 0;

  virtual std::string name() = 0;
};

struct SparsifyBooleans : public ColumnTransformer {
  std::string name() override { return "SparsifyBooleans"; }

  bool Matches(arrow::Field* field) override {
    return field->type()->Equals(arrow::boolean());
  }

  std::pair<std::shared_ptr<arrow::Field>, std::shared_ptr<arrow::ChunkedArray>>
  operator()(arrow::Field* field, arrow::ChunkedArray* chunked_array) override;
};

/// ConvertTimestamps parses RFC 3339 / ISO 8601 style timestamp strings into
/// Unix timestamp integers.
///
/// A timestamp string looks like:
///   1970-01-01 00:00:00+07:00 (RFC 3339)
///   1970-01-01 00:00:00Z      (RFC 3339)
///   1970-01-01T00:00:00+0700  (ISO 8601)
///   1970-01-01T00:00:00Z      (ISO 8601)
///
/// There are variations based on the presence or absence of the seconds field
/// and what separators are used between the date and time ("T" or " ") and in
/// the time zone offset (nothing or ":").
///
/// For compatibility between Spark, Pandas and Arrow, the timestamps produced
/// are nanoseconds since the beginning of the Unix epoch [1].
///
/// [1] https://arrow.apache.org/docs/python/timestamps.html
struct ConvertTimestamps : public ColumnTransformer {
  std::vector<std::string> property_names_;

  ConvertTimestamps(std::vector<std::string> property_names)
      : property_names_(std::move(property_names)) {}

  std::string name() override { return "ConvertTimestamps"; }

  bool Matches(arrow::Field* field) override {
    if (!field->type()->Equals(arrow::StringType())) {
      return false;
    }

    return std::find(
               property_names_.begin(), property_names_.end(), field->name()) !=
           property_names_.end();
  }

  std::pair<std::shared_ptr<arrow::Field>, std::shared_ptr<arrow::ChunkedArray>>
  operator()(arrow::Field* field, arrow::ChunkedArray* chunked_array) override;
};

void ApplyTransforms(
    galois::graphs::PropertyFileGraph* graph,
    const std::vector<std::unique_ptr<ColumnTransformer>>& transformers);

}  // namespace galois

#endif
