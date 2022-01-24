#ifndef KATANA_LIBGRAPH_KATANA_TOPOLOGYGENERATION_H_
#define KATANA_LIBGRAPH_KATANA_TOPOLOGYGENERATION_H_

#include <arrow/type_traits.h>

#include "katana/PropertyGraph.h"

namespace katana {

/*********************************************************/
/* Functions for generating pre-defined graph topologies */
/*********************************************************/

/// Generates a graph with the topology of a regular N x N grid, with diagonals
/// in every cell.
KATANA_EXPORT std::unique_ptr<katana::PropertyGraph> MakeGrid(
    size_t width, size_t height, bool with_diagonals) noexcept;

/// Generates a graph with the Ferris wheel topology: N - 1 nodes on the circle,
/// each connected to 2 neighbors on the circle and 1 central node.
KATANA_EXPORT std::unique_ptr<katana::PropertyGraph> MakeFerrisWheel(
    size_t num_nodes) noexcept;

/// Generates a graph with the sawtooth topology. Nodes are arranged into two rows.
/// First row has N nodes, second row has N+1 nodes. We connect ith-node in first row
/// with (ith and i+1th) nodes in second row.
KATANA_EXPORT std::unique_ptr<katana::PropertyGraph> MakeSawtooth(
    size_t length) noexcept;

/// Generates an N-clique.
KATANA_EXPORT std::unique_ptr<katana::PropertyGraph> MakeClique(
    size_t num_nodes) noexcept;

/// Generates a graph with the triangular array topology.
KATANA_EXPORT std::unique_ptr<katana::PropertyGraph> MakeTriangle(
    size_t num_rows) noexcept;

/***********************************************************/
/* Functions for adding node and edge properties to graphs */
/***********************************************************/

template <typename ValueFunc>
class KATANA_EXPORT PropertyGenerator;

namespace internal {

/// Statically maps \ref Input and \ref ValueFunc to the corresponding Arrow types, which
/// are used to build an Arrow table with the constructed properties.
template <typename Input, typename ValueFunc>
class PropertyGeneratorImpl {
  static_assert(
      std::is_invocable_v<ValueFunc, Input>,
      "PropertyGeneratorImpl must be constructed with an invocable type.");

  friend class PropertyGenerator<ValueFunc>;

public:
  using ValueType = std::invoke_result_t<ValueFunc, Input>;
  using ArrowType = typename arrow::CTypeTraits<ValueType>::ArrowType;
  using BuilderType = typename arrow::TypeTraits<ArrowType>::BuilderType;

  std::shared_ptr<arrow::Field> MakeField() const noexcept {
    std::shared_ptr<arrow::DataType> type = std::make_shared<ArrowType>();
    return arrow::field(name_, type);
  }

  std::shared_ptr<BuilderType> MakeBuilder() const noexcept {
    return std::make_shared<BuilderType>();
  }

  ValueType operator()(Input id) const { return value_func_(id); }

private:
  PropertyGeneratorImpl(const std::string& name, const ValueFunc& value_func)
      : name_(name), value_func_(value_func) {}

  const std::string& name_;
  const ValueFunc& value_func_;
};

// TMP helpers for compile-time checking of arguments passed to property adding functions.
template <typename T>
struct is_property_generator_impl : std::false_type {};

template <typename V>
struct is_property_generator_impl<PropertyGenerator<V>> : std::true_type {};

template <typename T>
struct is_property_generator : is_property_generator_impl<std::decay_t<T>> {};

template <typename... Ts>
struct all_property_generators
    : std::conjunction<is_property_generator<Ts>...> {};

template <bool is_node, typename... Args>
Result<void>
AddGraphProperties(
    PropertyGraph* pg, katana::TxnContext* txn_ctx, Args&&... generators) {
  static_assert(
      internal::all_property_generators<Args...>::value,
      "AddGraphProperties arguments except first must be of type "
      "PropertyGenerator.");

  using Node = PropertyGraph::Node;
  using Edge = PropertyGraph::Edge;
  using ArgType = std::conditional_t<is_node, Node, Edge>;

  // For every generator argument we add the corresponding field to the schema
  // and the corresponding column of node property values.
  std::vector<std::shared_ptr<arrow::Field>> fields;
  std::vector<std::shared_ptr<arrow::ChunkedArray>> columns;

  auto append_table_components = [&](auto&& generator_builder) -> Result<void> {
    auto generator = generator_builder.template Make<ArgType>();

    // For schema
    fields.emplace_back(generator.MakeField());

    // For property values
    auto builder = generator.MakeBuilder();
    if constexpr (is_node) {
      KATANA_CHECKED(builder->Reserve(pg->NumNodes()));
      for (Node n : pg->Nodes()) {
        KATANA_CHECKED(builder->Append(generator(n)));
      }
    } else {
      KATANA_CHECKED(builder->Reserve(pg->NumEdges()));
      for (Edge e : pg->OutEdges()) {
        KATANA_CHECKED(builder->Append(generator(e)));
      }
    }

    std::shared_ptr<arrow::Array> array;
    KATANA_CHECKED(builder->Finish(&array));

    // We anticipate that this API is going to be used for small synthetic graphs,
    // so columns are made up of a single chunk.
    columns.emplace_back(std::make_shared<arrow::ChunkedArray>(array));

    return katana::ResultSuccess();
  };

  // Iterate over all the supplied PropertyGenerators and build an arrow::Table.
  // We fold over || to terminate early if any call returns an error.
  Result<void> result = katana::ResultSuccess();
  ((result = append_table_components(std::forward<Args>(generators)),
    result.has_error()) ||
   ...);
  KATANA_CHECKED(result);

  auto schema = std::make_shared<arrow::Schema>(fields);
  auto table = arrow::Table::Make(schema, columns);

  if constexpr (is_node) {
    return pg->AddNodeProperties(table, txn_ctx);
  } else {
    return pg->AddEdgeProperties(table, txn_ctx);
  }
}

}  // namespace internal

/// Holds a name and value generating function for either a node or an edge property.
///
/// \tparam ValueFunc Type of the value generator function.
template <typename ValueFunc>
class KATANA_EXPORT PropertyGenerator {
public:
  /// \param name Property name
  /// \param value_func Value generator function, which accepts either a node or an edge id.
  PropertyGenerator(const std::string& name, const ValueFunc& value_func)
      : name_(name), value_func_(value_func) {}

  /// Constructs an implementation class object that statically maps \ref Input
  /// and \ref ValueFunc to the corresponding Arrow types.
  ///
  /// \tparam Input Type of the input arguments passed to the \ref ValueFunc object.
  template <typename Input>
  internal::PropertyGeneratorImpl<Input, ValueFunc> Make() {
    return {name_, value_func_};
  }

private:
  std::string name_;
  ValueFunc value_func_;
};

/// Convenience function to add node properties to pre-constructed property graphs.
/// It is a variadic function, it will add a node property for every provided PropertyGenerator.
///
/// For example:
///
/// AddNodeProperties(
///   pg,
///   PropertyGenerator("age",  [](Node id) { return static_cast<int32_t>(id * 2); }),
///   PropertyGenerator("name", [](Node id) { return fmt::format("Node {}", id); })
/// );
template <typename... Args>
KATANA_EXPORT Result<void>
AddNodeProperties(
    katana::PropertyGraph* pg, katana::TxnContext* txn_ctx,
    Args&&... generators) {
  return internal::AddGraphProperties<true>(
      pg, txn_ctx, std::forward<Args>(generators)...);
}

/// Convenience function to add edge properties to pre-constructed property graphs.
/// It is a variadic function, it will add an edge property for every passed PropertyGenerator.
///
/// For example:
///
/// AddEdgeProperties(
///   pg,
///   PropertyGenerator("average", [&pg](Edge id) {
///     Node src = pg->topology().edge_source(id);
///     Node dst = pg->topology().edge_dest(id);
///     return 0.5 * (src + dst);
///   }),
///   PropertyGenerator("name", [](Edge id) { return fmt::format("Edge {}", id); })
/// );
template <typename... Args>
KATANA_EXPORT Result<void>
AddEdgeProperties(
    katana::PropertyGraph* pg, katana::TxnContext* txn_ctx,
    Args&&... generators) {
  return internal::AddGraphProperties<false>(
      pg, txn_ctx, std::forward<Args>(generators)...);
}

}  // end namespace katana

#endif  // KATANA_LIBGRAPH_KATANA_TOPOLOGYGENERATION_H_
