#ifndef GALOIS_LIBGALOIS_TESTPROPERTYGRAPH_H_
#define GALOIS_LIBGALOIS_TESTPROPERTYGRAPH_H_

#include <arrow/api.h>
#include <arrow/type_traits.h>

#include "galois/Logging.h"
#include "galois/Random.h"
#include "galois/graphs/PropertyFileGraph.h"
#include "galois/graphs/PropertyGraph.h"

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

/// MakeFileGraph makes a file graph with the specified number of nodes and
/// properties and using the given topology policy.
///
/// \tparam ValueType is the type of column data
template <typename ValueType>
std::unique_ptr<galois::graphs::PropertyFileGraph>
MakeFileGraph(size_t num_nodes, size_t num_properties, Policy* policy) {
  std::vector<uint32_t> dests;
  std::vector<uint64_t> indices;

  indices.reserve(num_nodes);
  for (size_t i = 0; i < num_nodes; ++i) {
    std::vector<uint32_t> v = policy->GenerateNeighbors(i, num_nodes);
    std::copy(v.begin(), v.end(), std::back_inserter(dests));
    indices.push_back(dests.size());
  }

  auto g = std::make_unique<galois::graphs::PropertyFileGraph>();

  auto set_result = g->SetTopology(galois::graphs::GraphTopology{
      .out_indices =
          std::static_pointer_cast<arrow::UInt64Array>(BuildArray(indices)),
      .out_dests =
          std::static_pointer_cast<arrow::UInt32Array>(BuildArray(dests)),
  });
  GALOIS_LOG_ASSERT(set_result);

  size_t num_edges = dests.size();

  TableBuilder node_builder{num_nodes};
  TableBuilder edge_builder{num_edges};

  for (size_t i = 0; i < num_properties; ++i) {
    node_builder.AddColumn<ValueType>(ColumnOptions());
    edge_builder.AddColumn<ValueType>(ColumnOptions());
  }

  if (auto r = g->AddEdgeProperties(edge_builder.Finish()); !r) {
    GALOIS_LOG_FATAL("could not add edge property: {}", r.error());
  }
  if (auto r = g->AddNodeProperties(node_builder.Finish()); !r) {
    GALOIS_LOG_FATAL("could not add node property: {}", r.error());
  }

  return g;
}

/// BaselineIterate iterates over a property file graph with a standard "for
/// each node, for each edge" pattern and accesses the corresponding entries in
/// a node property and edge property array.
template <typename NodeType, typename EdgeType>
size_t BaselineIterate(galois::graphs::PropertyFileGraph* g,
                       int num_properties) {

  using NodeArrowType = typename galois::PropertyArrowType<NodeType>;
  using EdgeArrowType = typename galois::PropertyArrowType<EdgeType>;

  using NodeProperty = typename galois::PropertyArrowArrayType<NodeType>;
  using EdgeProperty = typename galois::PropertyArrowArrayType<EdgeType>;

  using NodePointer = typename arrow::TypeTraits<NodeArrowType>::CType*;
  using EdgePointer = typename arrow::TypeTraits<EdgeArrowType>::CType*;

  const auto* indices = g->topology().out_indices->raw_values();
  const auto* dests   = g->topology().out_dests->raw_values();

  std::vector<NodePointer> node_data;
  std::vector<EdgePointer> edge_data;

  for (int prop = 0; prop < num_properties; ++prop) {
    auto node_property = std::dynamic_pointer_cast<NodeProperty>(
        g->NodeProperty(prop)->chunk(0));
    auto edge_property = std::dynamic_pointer_cast<EdgeProperty>(
        g->EdgeProperty(prop)->chunk(0));

    GALOIS_LOG_ASSERT(node_property);
    GALOIS_LOG_ASSERT(edge_property);

    GALOIS_LOG_ASSERT(static_cast<size_t>(node_property->length()) ==
                      g->topology().num_nodes());
    GALOIS_LOG_ASSERT(static_cast<size_t>(edge_property->length()) ==
                      g->topology().num_edges());

    node_data.emplace_back(
        const_cast<NodePointer>(node_property->raw_values()));
    edge_data.emplace_back(
        const_cast<EdgePointer>(edge_property->raw_values()));
  }

  size_t result = 0;

  for (size_t i = 0, n = g->topology().num_nodes(); i < n; ++i) {
    uint64_t begin = (i == 0) ? 0 : indices[i - 1];
    uint64_t end   = indices[i];

    for (int prop = 0; prop < num_properties; ++prop) {
      result += node_data[prop][i];
    }

    for (; begin != end; ++begin) {
      for (int prop = 0; prop < num_properties; ++prop) {
        result += edge_data[prop][begin];
      }
      auto dest = dests[begin];
      for (int prop = 0; prop < num_properties; ++prop) {
        result += node_data[prop][dest];
      }
    }
  }

  return result;
}

template <size_t size, typename Graph>
struct SumNodeProperty {
  static size_t Call(Graph g, typename Graph::iterator node, size_t limit) {
    constexpr size_t total = std::tuple_size_v<typename Graph::node_properties>;
    constexpr size_t idx   = total - size;
    using Index =
        typename std::tuple_element_t<idx, typename Graph::node_properties>;

    auto v = g.template GetData<Index>(node);
    return v + SumNodeProperty<size - 1, Graph>::Call(g, node, limit);
  }
};

template <typename Graph>
struct SumNodeProperty<0, Graph> {
  static size_t Call(Graph, typename Graph::iterator, size_t) { return {}; }
};

/// Sum all the properties associated with a particular node.
template <typename Graph>
size_t SumNodePropertyV(Graph g, typename Graph::iterator node, size_t limit) {
  constexpr size_t size = std::tuple_size_v<typename Graph::node_properties>;
  return SumNodeProperty<size, Graph>::Call(g, node, limit);
}

template <size_t size, typename Graph>
struct SumEdgeProperty {
  static size_t Call(Graph g, typename Graph::edge_iterator edge,
                     size_t limit) {
    constexpr size_t total = std::tuple_size_v<typename Graph::edge_properties>;
    constexpr size_t idx   = total - size;
    using Index =
        typename std::tuple_element_t<idx, typename Graph::edge_properties>;

    auto v = g.template GetEdgeData<Index>(edge);
    return v + SumEdgeProperty<size - 1, Graph>::Call(g, edge, limit);
  }
};

template <typename Graph>
struct SumEdgeProperty<0, Graph> {
  static size_t Call(Graph, typename Graph::edge_iterator, size_t) {
    return {};
  }
};

/// Sum all the properties associated with a particular edge.
template <typename Graph>
size_t SumEdgePropertyV(Graph g, typename Graph::edge_iterator edge,
                        size_t limit) {
  constexpr size_t size = std::tuple_size_v<typename Graph::edge_properties>;
  return SumEdgeProperty<size, Graph>::Call(g, edge, limit);
}

template <typename NodeType, typename EdgeType>
size_t Iterate(galois::graphs::PropertyGraph<NodeType, EdgeType> g,
               size_t limit) {
  size_t result = 0;
  for (const auto& node : g) {
    result += SumNodePropertyV(g, node, limit);
    for (auto& edge : g.edges(node)) {
      result += SumEdgePropertyV(g, edge, limit);
      result += SumNodePropertyV(g, g.GetEdgeDest(edge), limit);
    }
  }

  return result;
}

/// ExpectedValue returns the value expected by Iterate or BaselineIterate
/// given the parameters to MakeFileGraph.
size_t ExpectedValue(size_t num_nodes, size_t num_edges, size_t num_properties,
                     bool ascending_values) {
  GALOIS_ASSERT(!ascending_values);

  return (num_nodes + 2 * num_edges) * num_properties;
}

#endif
