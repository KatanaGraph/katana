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
  TransformationView() = default;
  TransformationView(TransformationView&& other) = default;

  virtual ~TransformationView();

  /// Make a projected graph from a property graph. Shares state with
  /// the original graph.
  static std::unique_ptr<TransformationView> MakeProjectedGraph(
      const PropertyGraph& pg, const std::vector<std::string>& node_types,
      const std::vector<std::string>& edge_types);

  /// Return the number of nodes of the original property graph.
  uint64_t NumOriginalNodes() const final {
    return transformation_.original_to_transformed_nodes_.size();
  }

  /// Return the number of edges of the original property graph.
  uint64_t NumOriginalEdges() const final {
    return transformation_.original_to_transformed_edges_.size();
  }

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
    return transformation_.original_to_transformed_edges_[eid];
  }

  /// @param nid the input node id (must be original node id)
  Node OriginalToTransformedNodeId(const Node& nid) const noexcept {
    KATANA_LOG_DEBUG_ASSERT(nid < NumNodes());
    return transformation_.original_to_transformed_nodes_[nid];
  }

  /// Bitmask of nodes included in the transformation view. Should be used to construct arrow tables.
  std::shared_ptr<arrow::Buffer> NodeBitmask() const noexcept {
    auto length = transformation_.original_to_transformed_nodes_.size();
    return std::make_shared<arrow::Buffer>(
        transformation_.node_bitmask_data_.data(),
        arrow::BitUtil::BytesForBits(length));
  }

  /// Bitmask of edges included in the transformation view. Should be used to construct arrow tables.
  std::shared_ptr<arrow::Buffer> EdgeBitmask() const noexcept {
    auto length = transformation_.original_to_transformed_edges_.size();
    return std::make_shared<arrow::Buffer>(
        transformation_.edge_bitmask_data_.data(),
        arrow::BitUtil::BytesForBits(length));
  }

private:
  struct Transformation {
    NUMAArray<Node> original_to_transformed_nodes_;
    NUMAArray<Edge> original_to_transformed_edges_;
    NUMAArray<uint8_t> node_bitmask_data_;
    NUMAArray<uint8_t> edge_bitmask_data_;
  };

  TransformationView(
      const PropertyGraph& pg, GraphTopology&& projected_topo,
      Transformation&& transformation) noexcept
      : PropertyGraph(pg, std::move(projected_topo)),
        transformation_(std::move(transformation)) {}

  /// this function creates an empty projection with num_new_nodes nodes
  static std::unique_ptr<TransformationView> CreateEmptyEdgeProjectedTopology(
      const PropertyGraph& pg, uint32_t num_new_nodes,
      const DynamicBitset& bitset);

  /// this function creates an empty projection
  static std::unique_ptr<TransformationView> CreateEmptyProjectedTopology(
      const PropertyGraph& pg, const DynamicBitset& bitset);

  // Data
  Transformation transformation_{};
};
}  // namespace katana

#endif
