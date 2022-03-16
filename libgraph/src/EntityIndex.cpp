#include "katana/EntityIndex.h"

#include "katana/PropertyGraph.h"

namespace katana {

// Switch statement over creation of per-type indexes.
template <typename node_or_edge>
Result<std::unique_ptr<EntityIndex<typename node_or_edge::underlying_type>>>
MakeTypedEntityIndex(
    const std::string& property_name, size_t num_entities,
    std::shared_ptr<arrow::Array> property) {

  using node_or_edge_int = typename node_or_edge::underlying_type;
  std::unique_ptr<EntityIndex<node_or_edge_int>> index;

  switch (property->type_id()) {
  case arrow::Type::BOOL:
    index = std::make_unique<PrimitiveEntityIndex<node_or_edge_int, bool>>(
        property_name, num_entities, property);
    break;
  case arrow::Type::UINT8:
    index = std::make_unique<PrimitiveEntityIndex<node_or_edge_int, uint8_t>>(
        property_name, num_entities, property);
    break;
  case arrow::Type::INT16:
    index = std::make_unique<PrimitiveEntityIndex<node_or_edge, int16_t>>(
        property_name, num_entities, property);
    break;
  case arrow::Type::INT32:
    index = std::make_unique<PrimitiveEntityIndex<node_or_edge, int32_t>>(
        property_name, num_entities, property);
    break;
  case arrow::Type::INT64:
    index = std::make_unique<PrimitiveEntityIndex<node_or_edge_int, int64_t>>(
        property_name, num_entities, property);
    break;
  case arrow::Type::UINT64:
    index = std::make_unique<PrimitiveEntityIndex<node_or_edge_int, uint64_t>>(
        property_name, num_entities, property);
    break;
  case arrow::Type::DOUBLE:
    index = std::make_unique<PrimitiveEntityIndex<node_or_edge_int, double_t>>(
        property_name, num_entities, property);
    break;
  case arrow::Type::FLOAT:
    index = std::make_unique<PrimitiveEntityIndex<node_or_edge, float_t>>(
        property_name, num_entities, property);
    break;
  case arrow::Type::LARGE_STRING:
    index = std::make_unique<StringEntityIndex<node_or_edge_int>>(
        property_name, num_entities, property);
    break;
  default:
    return KATANA_ERROR(
        ErrorCode::InvalidArgument, "Column has type unknown for indexing: {}",
        property->type()->ToString());
  }

  return katana::MakeResult(std::move(index));
}

template <typename node_or_edge, typename c_type>
Result<void>
PrimitiveEntityIndex<node_or_edge, c_type>::BuildFromProperty() {
  if (static_cast<uint64_t>(property_->length()) < num_entities_) {
    return KATANA_ERROR(
        ErrorCode::InvalidArgument, "Property does not contain all entities");
  }

  // TODO(osh): Index build should be parallelized.
  for (node_or_edge i = 0; i < num_entities_; ++i) {
    // The keys inserted are the node ids - the set translates these into
    // property values.
    if (property_->IsValid(i)) {
      set_.insert(IndexID{i});
    }
  }

  return katana::ResultSuccess();
}

template <typename node_or_edge>
Result<void>
StringEntityIndex<node_or_edge>::BuildFromProperty() {
  if (static_cast<uint64_t>(property_->length()) < num_entities_) {
    return KATANA_ERROR(
        ErrorCode::InvalidArgument, "Property does not contain all entities");
  }

  // TODO(osh): Index build should be parallelized.
  for (node_or_edge i = 0; i < num_entities_; ++i) {
    // The keys inserted are the node ids - the set translates these into
    // property values.
    if (property_->IsValid(i)) {
      set_.insert(IndexID{i});
    }
  }

  return katana::ResultSuccess();
}

// Forward declare template types to allow implementation in .cpp.
template class PrimitiveEntityIndex<GraphTopology::Node::underlying_type, bool>;
template class PrimitiveEntityIndex<GraphTopology::Edge::underlying_type, bool>;
template class PrimitiveEntityIndex<GraphTopology::Node::underlying_type, uint8_t>;
template class PrimitiveEntityIndex<GraphTopology::Edge::underlying_type, uint8_t>;
template class PrimitiveEntityIndex<GraphTopology::Node::underlying_type, int16_t>;
template class PrimitiveEntityIndex<GraphTopology::Edge::underlying_type, int16_t>;
template class PrimitiveEntityIndex<GraphTopology::Node::underlying_type, int32_t>;
template class PrimitiveEntityIndex<GraphTopology::Edge::underlying_type, int32_t>;
template class PrimitiveEntityIndex<GraphTopology::Node::underlying_type, int64_t>;
template class PrimitiveEntityIndex<GraphTopology::Edge::underlying_type, int64_t>;
template class PrimitiveEntityIndex<GraphTopology::Node::underlying_type, uint64_t>;
template class PrimitiveEntityIndex<GraphTopology::Edge::underlying_type, uint64_t>;
template class PrimitiveEntityIndex<GraphTopology::Node::underlying_type, double_t>;
template class PrimitiveEntityIndex<GraphTopology::Edge::underlying_type, double_t>;
template class PrimitiveEntityIndex<GraphTopology::Node::underlying_type, float_t>;
template class PrimitiveEntityIndex<GraphTopology::Edge::underlying_type, float_t>;

template class StringEntityIndex<GraphTopology::Node::underlying_type>;
template class StringEntityIndex<GraphTopology::Edge::underlying_type>;

template Result<std::unique_ptr<EntityIndex<GraphTopology::Node::underlying_type>>>
MakeTypedEntityIndex<GraphTopology::Node>(
    const std::string& property_name, size_t num_entities,
    std::shared_ptr<arrow::Array> property);
template Result<std::unique_ptr<EntityIndex<GraphTopology::Edge::underlying_type>>>
MakeTypedEntityIndex<GraphTopology::Edge>(
    const std::string& property_name, size_t num_entities,
    std::shared_ptr<arrow::Array> property);

}  // namespace katana
