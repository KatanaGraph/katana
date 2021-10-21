#ifndef KATANA_LIBGALOIS_KATANA_TOPOLOGYGENERATION_H_
#define KATANA_LIBGALOIS_KATANA_TOPOLOGYGENERATION_H_

#include <arrow/type_traits.h>

#include "katana/PropertyGraph.h"

namespace katana {

/*********************************************************/
/* Functions for generating pre-defined graph tolologies */
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

namespace internal {

template <typename Input, typename F>
class PropertySetter {
  static_assert(
      std::is_invocable_v<F, Input>,
      "PropertySetter must be constructed with an invokable type.");

public:
  using ValueType = std::invoke_result_t<F, Input>;
  using ArrowType = typename arrow::CTypeTraits<ValueType>::ArrowType;
  using BuilderType = typename arrow::TypeTraits<ArrowType>::BuilderType;

  PropertySetter(std::string name, F resolve_value)
      : name(std::move(name)), resolve_value(std::move(resolve_value)) {}

  std::shared_ptr<arrow::Field> Field() const noexcept {
    std::shared_ptr<arrow::DataType> type = std::make_shared<ArrowType>();
    return arrow::field(name, type);
  }

  std::shared_ptr<BuilderType> Builder() const noexcept {
    return std::make_shared<BuilderType>();
  }

  ValueType operator()(Input id) const { return resolve_value(id); }

private:
  std::string name;
  F resolve_value;
};

// TMP helpers for compile-time checking of arguments passed to property adding functions.
template <typename T>
struct is_property_setter_impl : std::false_type {};

template <typename I, typename O>
struct is_property_setter_impl<PropertySetter<I, O>> : std::true_type {};

template <typename T>
struct is_property_setter : is_property_setter_impl<std::decay_t<T>> {};

template <typename... Ts>
struct all_property_setters : std::conjunction<is_property_setter<Ts>...> {};

template <bool is_node, typename... Args>
Result<void>
AddGraphProperties(katana::PropertyGraph* pg, Args&&... setters) {
  static_assert(
      internal::all_property_setters<Args...>::value,
      "AddGraphProperties arguments except first must be of type "
      "PropertySetter.");

  // For every setter argument we add the corresponding field to the schema
  // and the corresponding column of node property values.
  std::vector<std::shared_ptr<arrow::Field>> fields;
  std::vector<std::shared_ptr<arrow::ChunkedArray>> columns;

  auto append_table_components = [&](const auto& setter) -> Result<void> {
    // For schema
    fields.emplace_back(setter.Field());

    // For property values
    auto builder = setter.Builder();
    if constexpr (is_node) {
      builder->Reserve(pg->num_nodes());
      for (auto node_it = pg->begin(); node_it != pg->end(); ++node_it) {
        KATANA_CHECKED(builder->Append(setter(*node_it)));
      }
    } else {
      builder->Reserve(pg->num_edges());
      auto edges = pg->topology().all_edges();
      for (auto edge_it = edges.begin(); edge_it != edges.end(); ++edge_it) {
        KATANA_CHECKED(builder->Append(setter(*edge_it)));
      }
    }

    std::shared_ptr<arrow::Array> array;
    KATANA_CHECKED(builder->Finish(&array));

    // We anticipate that this API is going to be used for small synthetic graphs,
    // so columns are made up of a single chunk.
    columns.emplace_back(std::make_shared<arrow::ChunkedArray>(array));

    return katana::ResultSuccess();
  };

  // Iterate over all the supplied PropertySetters and build an arrow::Table.
  // We fold over || to terminate early if any call returns an error.
  Result<void> result = katana::ResultSuccess();
  ((result = append_table_components(std::forward<Args>(setters)),
    result.has_error()) ||
   ...);
  KATANA_CHECKED(result);

  auto schema = std::make_shared<arrow::Schema>(fields);
  auto table = arrow::Table::Make(schema, columns);

  if constexpr (is_node) {
    return pg->AddNodeProperties(table);
  } else {
    return pg->AddEdgeProperties(table);
  }
}

}  // namespace internal

template <typename F>
KATANA_EXPORT internal::PropertySetter<PropertyGraph::Node, F>
NodePropertySetter(std::string name, F resolve_value) {
  return {std::move(name), std::move(resolve_value)};
}

template <typename F>
KATANA_EXPORT internal::PropertySetter<PropertyGraph::Edge, F>
EdgePropertySetter(std::string name, F resolve_value) {
  return {std::move(name), std::move(resolve_value)};
}

template <typename... Args>
KATANA_EXPORT Result<void>
AddNodeProperties(katana::PropertyGraph* pg, Args&&... setters) {
  return internal::AddGraphProperties<true>(pg, std::forward<Args>(setters)...);
}

template <typename... Args>
KATANA_EXPORT Result<void>
AddEdgeProperties(katana::PropertyGraph* pg, Args&&... setters) {
  return internal::AddGraphProperties<false>(
      pg, std::forward<Args>(setters)...);
}

}  // end namespace katana

#endif  // KATANA_LIBGALOIS_KATANA_TOPOLOGYGENERATION_H_
