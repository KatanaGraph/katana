#include "katana/PropertyIndex.h"

namespace katana {

// Helpers for retrieving state for either nodes or edges.
template <typename node_or_edge>
class NodeOrEdge {
public:
  static std::shared_ptr<arrow::ChunkedArray> GetProperty(
      PropertyGraph* pg, int field_idx);
  static std::shared_ptr<arrow::Schema> GetSchema(PropertyGraph* pg);
};

// Main index building function.
template <typename node_or_edge>
Result<std::unique_ptr<PropertyIndex<node_or_edge>>>
PropertyIndex<node_or_edge>::Make(
    PropertyGraph* pg, const std::string& column) {
  std::shared_ptr<arrow::Schema> schema =
      NodeOrEdge<node_or_edge>::GetSchema(pg);
  int field_idx = schema->GetFieldIndex(column);
  if (field_idx == -1) {
    return KATANA_ERROR(ErrorCode::NotFound, "No such property: {}", column);
  }

  // Get a view of the property.
  std::shared_ptr<arrow::ChunkedArray> property =
      NodeOrEdge<node_or_edge>::GetProperty(pg, field_idx);

  // Create an index based on the type of the field.
  std::unique_ptr<PropertyIndex<node_or_edge>> index;
  switch (schema->field(field_idx)->type()->id()) {
  case arrow::Type::INT32:
    index =
        std::make_unique<BasicPropertyIndex<node_or_edge, int32_t>>(pg, column);
    break;
  case arrow::Type::INT64:
    index =
        std::make_unique<BasicPropertyIndex<node_or_edge, int64_t>>(pg, column);
    break;
  default:
    return KATANA_ERROR(
        ErrorCode::InvalidArgument,
        "Column {} has type unknown for indexing: {}", column,
        schema->field(field_idx)->type()->ToString());
  }

  Result<void> build_result = index->BuildFromProperty(property);
  if (!build_result) {
    return build_result.error();
  }

  return index;
}

template <typename node_or_edge, typename key_type>
Result<void>
BasicPropertyIndex<node_or_edge, key_type>::BuildFromProperty(
    std::shared_ptr<arrow::ChunkedArray> property) {
  (void)property;
  return KATANA_ERROR(ErrorCode::InvalidArgument, "build error");
}

template <>
std::shared_ptr<arrow::ChunkedArray>
NodeOrEdge<GraphTopology::Node>::GetProperty(PropertyGraph* pg, int field_idx) {
  return pg->GetNodeProperty(field_idx);
}

template <>
std::shared_ptr<arrow::ChunkedArray>
NodeOrEdge<GraphTopology::Edge>::GetProperty(PropertyGraph* pg, int field_idx) {
  return pg->GetEdgeProperty(field_idx);
}

template <>
std::shared_ptr<arrow::Schema>
NodeOrEdge<GraphTopology::Node>::GetSchema(PropertyGraph* pg) {
  return pg->node_schema();
}

template <>
std::shared_ptr<arrow::Schema>
NodeOrEdge<GraphTopology::Edge>::GetSchema(PropertyGraph* pg) {
  return pg->edge_schema();
}

// Forward declare template types to allow implementation in .cpp.
template class PropertyIndex<GraphTopology::Node>;
template class PropertyIndex<GraphTopology::Edge>;

template class BasicPropertyIndex<GraphTopology::Node, int32_t>;
template class BasicPropertyIndex<GraphTopology::Edge, int32_t>;
template class BasicPropertyIndex<GraphTopology::Node, int64_t>;
template class BasicPropertyIndex<GraphTopology::Edge, int64_t>;

}  // namespace katana