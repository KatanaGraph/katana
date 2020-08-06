#ifndef GALOIS_LIBGALOIS_TESTPROPERTYGRAPH_H_
#define GALOIS_LIBGALOIS_TESTPROPERTYGRAPH_H_

#include <arrow/api.h>
#include <arrow/type_traits.h>

#include "galois/Logging.h"
#include "galois/Random.h"

/// Generate property graphs for testing.
///
/// \file TestPropertyGraph.h

/// BuildArray copies the input data into an arrow array
template <typename T>
std::shared_ptr<arrow::Array> BuildArray(std::vector<T>& data) {
  using Builder = typename arrow::CTypeTraits<T>::BuilderType;

  Builder builder;
  auto append_status = builder.AppendValues(data);
  GALOIS_LOG_ASSERT(append_status.ok());

  std::shared_ptr<arrow::Array> array;
  auto finish_status = builder.Finish(&array);
  GALOIS_LOG_ASSERT(finish_status.ok());
  return array;
}

struct ColumnOptions {
  std::string name;
  size_t chunk_size{~size_t{0}};
  bool ascending_values{false};
};

/// TableBuilder builds tables with various data types but with a fixed value
/// distribution. It is mainly for making inputs for testing and benchmarking.
class TableBuilder {
  size_t size_{0};
  std::vector<std::shared_ptr<arrow::ChunkedArray>> columns_;
  std::vector<std::shared_ptr<arrow::Field>> fields_;

public:
  TableBuilder(size_t size) : size_(size) {}

  template <typename T>
  void AddColumn(const ColumnOptions& options);

  template <typename T>
  void AddColumn() {
    return AddColumn<T>(ColumnOptions());
  }

  std::shared_ptr<arrow::Table> Finish();
};

std::shared_ptr<arrow::Table> TableBuilder::Finish() {
  auto ret = arrow::Table::Make(arrow::schema(fields_), columns_);
  columns_.clear();
  fields_.clear();
  return ret;
}

template <typename T>
void TableBuilder::AddColumn(const ColumnOptions& options) {
  using Builder   = typename arrow::CTypeTraits<T>::BuilderType;
  using ArrowType = typename arrow::CTypeTraits<T>::ArrowType;

  std::vector<std::shared_ptr<arrow::Array>> chunks;

  std::vector<T> data;
  T value{};

  for (size_t chunk_index = 0, idx = 0; idx < size_; ++idx, ++value) {
    if (options.ascending_values) {
      data.emplace_back(value);
    } else {
      data.emplace_back(1);
    }
    bool last_in_chunk = (chunk_index + 1 >= options.chunk_size);
    bool last          = (idx + 1 >= size_);

    if (!last_in_chunk && !last) {
      ++chunk_index;
      continue;
    }

    Builder builder;
    auto append_status = builder.AppendValues(data);
    GALOIS_LOG_ASSERT(append_status.ok());

    data.clear();

    std::shared_ptr<arrow::Array> array;
    auto finish_status = builder.Finish(&array);
    GALOIS_LOG_ASSERT(finish_status.ok());

    chunks.emplace_back(std::move(array));
  }

  std::string name = options.name;
  if (name.empty()) {
    name = std::to_string(fields_.size());
  }

  fields_.emplace_back(arrow::field(name, std::make_shared<ArrowType>()));
  columns_.emplace_back(std::make_shared<arrow::ChunkedArray>(chunks));
}

class Policy {
public:
  virtual ~Policy() = default;

  virtual std::vector<uint32_t> GenerateNeighbors(size_t node_id,
                                                  size_t num_nodes) = 0;
};

class LinePolicy : public Policy {
  size_t width_{};

public:
  LinePolicy(size_t width) : width_(width) {}

  std::vector<uint32_t> GenerateNeighbors(size_t node_id,
                                          size_t num_nodes) override {
    std::vector<uint32_t> r;
    for (size_t i = 0; i < width_; ++i) {
      size_t neighbor = (node_id + i + 1) % num_nodes;
      r.emplace_back(neighbor);
    }
    return r;
  }
};

class RandomPolicy : public Policy {
  size_t width_{};

public:
  RandomPolicy(size_t width) : width_(width) {}

  std::vector<uint32_t> GenerateNeighbors([[maybe_unused]] size_t node_id,
                                          size_t num_nodes) override {
    std::vector<uint32_t> r;
    for (size_t i = 0; i < width_; ++i) {
      size_t neighbor = galois::RandomUniformInt(num_nodes);
      r.emplace_back(neighbor);
    }
    return r;
  }
};

#endif
