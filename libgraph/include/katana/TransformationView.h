#ifndef KATANA_LIBGRAPH_KATANA_TRANSFORMATIONVIEW_H_
#define KATANA_LIBGRAPH_KATANA_TRANSFORMATIONVIEW_H_

#include <memory>
#include <utility>

#include "katana/PropertyGraph.h"

namespace katana {

/// A TransformationView is a topological transformation of the property graph.
/// It is derived from the PropertyGraph so that it can be treated by query and
/// analytics routines as a regular property graph.
class KATANA_EXPORT TransformationView final : public PropertyGraph {
public:
  // Virtual methods
public:
  ~TransformationView() override;

private:
  /// Bitmask of nodes included in the transformation view. Should be used to construct arrow tables.
  std::shared_ptr<arrow::Buffer> NodeBitmask() const noexcept override {
    auto length = original_to_transformed_nodes_.size();
    return std::make_shared<arrow::Buffer>(
        node_bitmask_data_.data(), arrow::BitUtil::BytesForBits(length));
  }

  /// Bitmask of edges included in the transformation view. Should be used to construct arrow tables.
  std::shared_ptr<arrow::Buffer> EdgeBitmask() const noexcept override {
    auto length = original_to_transformed_edges_.size();
    return std::make_shared<arrow::Buffer>(
        edge_bitmask_data_.data(), arrow::BitUtil::BytesForBits(length));
  }

  /// Return the number of nodes of the original property graph.
  uint64_t NumOriginalNodes() const override {
    return original_to_transformed_nodes_.size();
  }

  /// Return the number of edges of the original property graph.
  uint64_t NumOriginalEdges() const override {
    return original_to_transformed_edges_.size();
  }

  Result<RDGTopology*> LoadTopology(const RDGTopology& shadow) override;

  // Regular methods
public:
  TransformationView() = default;
  TransformationView(TransformationView&& other) = default;

  /// Make a projected graph from a property graph. Shares state with
  /// the original graph.
  static std::unique_ptr<TransformationView> MakeProjectedGraph(
      const PropertyGraph& pg, const std::vector<std::string>& node_types,
      const std::vector<std::string>& edge_types);

  /// @param eid the input eid (must be projected edge id)
  Edge TransformedToOriginalEdgeId(const Edge& eid) const noexcept {
    KATANA_LOG_DEBUG_ASSERT(eid < NumEdges());
    return topology().GetLocalEdgeIDFromOutEdge(eid);
  }

  /// @param nid the input node id (must be projected node id)
  Node TransformedToOriginalNodeId(const Node& nid) const noexcept {
    KATANA_LOG_DEBUG_ASSERT(nid < NumNodes());
    return topology().GetLocalNodeID(nid);
  }

  /// @param eid the input eid (must be original edge id)
  Edge OriginalToTransformedEdgeId(const Edge& eid) const noexcept {
    KATANA_LOG_DEBUG_ASSERT(eid < NumEdges());
    return original_to_transformed_edges_[eid];
  }

  /// @param nid the input node id (must be original node id)
  Node OriginalToTransformedNodeId(const Node& nid) const noexcept {
    KATANA_LOG_DEBUG_ASSERT(nid < NumNodes());
    return original_to_transformed_nodes_[nid];
  }

private:
  TransformationView(
      const PropertyGraph& pg, GraphTopology&& projected_topo,
      NUMAArray<Node>&& original_to_transformed_nodes,
      NUMAArray<Edge>&& original_to_transformed_edges,
      NUMAArray<uint8_t>&& node_bitmask_data,
      NUMAArray<uint8_t>&& edge_bitmask_data) noexcept
      : PropertyGraph(pg, std::move(projected_topo)),
        original_to_transformed_nodes_(
            std::move(original_to_transformed_nodes)),
        original_to_transformed_edges_(
            std::move(original_to_transformed_edges)),
        node_bitmask_data_(std::move(node_bitmask_data)),
        edge_bitmask_data_(std::move(edge_bitmask_data)) {}

  /// this function creates an empty projection with num_new_nodes nodes
  static std::unique_ptr<TransformationView> CreateEmptyEdgeProjectedTopology(
      const PropertyGraph& pg, uint32_t num_new_nodes,
      const DynamicBitset& bitset);

  /// this function creates an empty projection
  static std::unique_ptr<TransformationView> CreateEmptyProjectedTopology(
      const PropertyGraph& pg, const DynamicBitset& bitset);

  // Data
  NUMAArray<Node> original_to_transformed_nodes_;
  NUMAArray<Edge> original_to_transformed_edges_;

  // TODO(yan): Promote bitmasks to the PropertyGraph class to be able to construct
  // transformation views on other transformation views.
  NUMAArray<uint8_t> node_bitmask_data_;
  NUMAArray<uint8_t> edge_bitmask_data_;
};
}  // namespace katana

#endif
