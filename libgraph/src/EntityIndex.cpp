#include "katana/EntityIndex.h"

#include "katana/PropertyGraph.h"

namespace katana {

// Switch statement over creation of per-type indexes.
template <typename node_or_edge>
Result<std::unique_ptr<EntityIndex<node_or_edge>>>
MakeTypedEntityIndex(
    const std::string& property_name, size_t num_entities,
    std::shared_ptr<arrow::Array> property) {
  std::unique_ptr<EntityIndex<node_or_edge>> index;

  switch (property->type_id()) {
  case arrow::Type::BOOL:
    index = std::make_unique<PrimitiveEntityIndex<node_or_edge, bool>>(
        property_name, num_entities, property);
    break;
  case arrow::Type::UINT8:
    index = std::make_unique<PrimitiveEntityIndex<node_or_edge, uint8_t>>(
        property_name, num_entities, property);
    break;
  case arrow::Type::INT64:
    index = std::make_unique<PrimitiveEntityIndex<node_or_edge, int64_t>>(
        property_name, num_entities, property);
    break;
  case arrow::Type::UINT64:
    index = std::make_unique<PrimitiveEntityIndex<node_or_edge, uint64_t>>(
        property_name, num_entities, property);
    break;
  case arrow::Type::DOUBLE:
    index = std::make_unique<PrimitiveEntityIndex<node_or_edge, double_t>>(
        property_name, num_entities, property);
    break;
  case arrow::Type::LARGE_STRING:
    index = std::make_unique<StringEntityIndex<node_or_edge>>(
        property_name, num_entities, property);
    break;
  default:
    return KATANA_ERROR(
        ErrorCode::InvalidArgument, "Column has type unknown for indexing: {}",
        property->type()->ToString());
  }

  // Some compilers seem to have trouble converting to Result here.
  return Result<std::unique_ptr<EntityIndex<node_or_edge>>>(std::move(index));
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
template class PrimitiveEntityIndex<GraphTopology::Node, bool>;
template class PrimitiveEntityIndex<GraphTopology::Edge, bool>;
template class PrimitiveEntityIndex<GraphTopology::Node, uint8_t>;
template class PrimitiveEntityIndex<GraphTopology::Edge, uint8_t>;
template class PrimitiveEntityIndex<GraphTopology::Node, int64_t>;
template class PrimitiveEntityIndex<GraphTopology::Edge, int64_t>;
template class PrimitiveEntityIndex<GraphTopology::Node, uint64_t>;
template class PrimitiveEntityIndex<GraphTopology::Edge, uint64_t>;
template class PrimitiveEntityIndex<GraphTopology::Node, double_t>;
template class PrimitiveEntityIndex<GraphTopology::Edge, double_t>;

template class StringEntityIndex<GraphTopology::Node>;
template class StringEntityIndex<GraphTopology::Edge>;

template Result<std::unique_ptr<EntityIndex<GraphTopology::Node>>>
MakeTypedEntityIndex(
    const std::string& property_name, size_t num_entities,
    std::shared_ptr<arrow::Array> property);
template Result<std::unique_ptr<EntityIndex<GraphTopology::Edge>>>
MakeTypedEntityIndex(
    const std::string& property_name, size_t num_entities,
    std::shared_ptr<arrow::Array> property);

}  // namespace katana
