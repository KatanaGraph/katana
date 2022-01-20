#ifndef KATANA_LIBGRAPH_KATANA_TYPEDPROPERTYGRAPH_H_
#define KATANA_LIBGRAPH_KATANA_TYPEDPROPERTYGRAPH_H_

#include <tuple>

#include <arrow/type_fwd.h>
#include <boost/iterator/counting_iterator.hpp>

#include "katana/Details.h"
#include "katana/NoDerefIterator.h"
#include "katana/Properties.h"
#include "katana/PropertyGraph.h"
#include "katana/PropertyViews.h"
#include "katana/Result.h"
#include "katana/Traits.h"

namespace katana {

/// A property graph is a graph that has properties associated with its nodes
/// and edges. A property has a name and value. Its value may be a primitive
/// type, a list of values or a composition of properties.
///
/// A TypedPropertyGraph is a representation of a property graph that imposes a
/// typed view on top of an underlying \ref PropertyGraph. A
/// PropertyGraph is appropriate for cases where the graph is largely
/// uninterpreted and can be manipulated as a collection of bits. A
/// TypedPropertyGraph is appropriate for cases where computation needs to be done
/// on the properties themselves.
///
/// \tparam NodeProps A tuple of property types (\ref Properties.h) for nodes
/// \tparam EdgeProps A tuple of property types for edges
template <typename NodeProps, typename EdgeProps>
class TypedPropertyGraph {
  using NodeView = PropertyViewTuple<NodeProps>;
  using EdgeView = PropertyViewTuple<EdgeProps>;

  PropertyGraph* pg_;

  NodeView node_view_;
  EdgeView edge_view_;

  TypedPropertyGraph(PropertyGraph* pg, NodeView node_view, EdgeView edge_view)
      : pg_(pg),
        node_view_(std::move(node_view)),
        edge_view_(std::move(edge_view)) {}

public:
  using node_properties = NodeProps;
  using edge_properties = EdgeProps;
  using node_iterator = GraphTopology::node_iterator;
  using edge_iterator = GraphTopology::edge_iterator;
  using edges_range = GraphTopology::edges_range;
  using nodes_range = GraphTopology::nodes_range;
  using iterator = GraphTopology::iterator;
  using Node = GraphTopology::Node;
  using Edge = GraphTopology::Edge;

  // Standard container concepts

  node_iterator begin() const { return pg_->begin(); }

  node_iterator end() const { return pg_->end(); }

  size_t size() const { return pg_->size(); }

  bool empty() const { return pg_->empty(); }

  // Graph accessors

  /**
   * Gets the node data.
   *
   * @param node node to get the data of
   * @returns reference to the node data
   */
  template <typename NodeIndex>
  PropertyReferenceType<NodeIndex> GetData(const Node& node) {
    constexpr size_t prop_col_index = find_trait<NodeIndex, NodeProps>();
    return std::get<prop_col_index>(node_view_)
        .GetValue(pg_->GetNodePropertyIndex(node));
  }
  template <typename NodeIndex>
  PropertyReferenceType<NodeIndex> GetData(const node_iterator& node) {
    return GetData<NodeIndex>(*node);
  }

  /**
   * Gets the node data.
   *
   * @param node node to get the data of
   * @returns const reference to the node data
   */
  template <typename NodeIndex>
  PropertyConstReferenceType<NodeIndex> GetData(const Node& node) const {
    constexpr size_t prop_col_index = find_trait<NodeIndex, NodeProps>();
    return std::get<prop_col_index>(node_view_)
        .GetValue(pg_->GetNodePropertyIndex(node));
  }
  template <typename NodeIndex>
  PropertyConstReferenceType<NodeIndex> GetData(
      const node_iterator& node) const {
    return GetData<NodeIndex>(*node);
  }

  /**
   * Gets the edge data.
   *
   * @param edge edge iterator to get the data of
   * @returns reference to the edge data
   */
  template <typename EdgeIndex>
  PropertyReferenceType<EdgeIndex> GetEdgeData(const edge_iterator& edge) {
    constexpr size_t prop_col_index = find_trait<EdgeIndex, EdgeProps>();
    return std::get<prop_col_index>(edge_view_)
        .GetValue(pg_->GetEdgePropertyIndexFromOutEdge(*edge));
  }

  /**
   * Gets the edge data.
   *
   * @param edge edge iterator to get the data of
   * @returns const reference to the edge data
   */
  template <typename EdgeIndex>
  PropertyConstReferenceType<EdgeIndex> GetEdgeData(
      const edge_iterator& edge) const {
    constexpr size_t prop_col_index = find_trait<EdgeIndex, EdgeProps>();
    return std::get<prop_col_index>(edge_view_)
        .GetValue(pg_->GetEdgePropertyIndexFromOutEdge(*edge));
  }

  /**
   * Gets the destination for an edge.
   *
   * @param edge edge id to get the destination of
   * @returns node id of the edge destination
   */
  Node OutEdgeDst(Edge e) const noexcept {
    return pg_->topology().OutEdgeDst(e);
  }

  size_t OutDegree(Node n) const noexcept {
    return pg_->topology().OutDegree(n);
  }

  uint64_t NumNodes() const { return pg_->NumNodes(); }
  uint64_t NumEdges() const { return pg_->NumEdges(); }

  /**
   * Gets all out-edges.
   *
   * @returns iterable edge range for the entire graph.
   */
  edges_range OutEdges() const noexcept { return pg_->topology().OutEdges(); }

  /**
   * Gets the edge range of some node.
   *
   * @param node node to get the edge range of
   * @returns iterable edge range for node.
   */
  edges_range OutEdges(Node node) const { return pg_->OutEdges(node); }

  nodes_range Nodes() const noexcept { return pg_->topology().Nodes(); }
  /**
   * Accessor for the underlying PropertyGraph.
   *
   * @returns pointer to the underlying PropertyGraph.
   */
  const PropertyGraph& GetPropertyGraph() const { return *pg_; }

  // Graph constructors
  static Result<TypedPropertyGraph<NodeProps, EdgeProps>> Make(
      PropertyGraph* pg, const std::vector<std::string>& node_properties,
      const std::vector<std::string>& edge_properties);
  static Result<TypedPropertyGraph<NodeProps, EdgeProps>> Make(
      PropertyGraph* pg);
};

template <typename PGView, typename NodeProps, typename EdgeProps>
class TypedPropertyGraphView : public PGView {
  using NodeView = PropertyViewTuple<NodeProps>;
  using EdgeView = PropertyViewTuple<EdgeProps>;

  NodeView node_view_;
  EdgeView edge_view_;

  TypedPropertyGraphView(
      const PGView& pg_view, NodeView&& node_view, EdgeView&& edge_view)
      : PGView(pg_view),
        node_view_(std::move(node_view)),
        edge_view_(std::move(edge_view)) {}

public:
  using node_properties = NodeProps;
  using edge_properties = EdgeProps;
  using node_iterator = typename PGView::node_iterator;
  using edge_iterator = typename PGView::edge_iterator;
  using edges_range = typename PGView::edges_range;
  using iterator = typename PGView::iterator;
  using Node = typename PGView::Node;
  using Edge = typename PGView::Edge;

  /**
   * Gets the node data.
   *
   * @param node node to get the data of
   * @returns reference to the node data
   */
  template <typename NodeIndex>
  PropertyReferenceType<NodeIndex> GetData(const Node& node) {
    constexpr size_t prop_col_index = find_trait<NodeIndex, NodeProps>();
    return std::get<prop_col_index>(node_view_)
        .GetValue(PGView::GetNodePropertyIndex(node));
  }

  /**
   * Gets the node data.
   *
   * @param node node to get the data of
   * @returns const reference to the node data
   */
  template <typename NodeIndex>
  PropertyConstReferenceType<NodeIndex> GetData(const Node& node) const {
    constexpr size_t prop_col_index = find_trait<NodeIndex, NodeProps>();
    return std::get<prop_col_index>(node_view_)
        .GetValue(PGView::GetNodePropertyIndex(node));
  }

  /**
   * Gets the edge data.
   *
   * @param edge edge iterator to get the data of
   * @returns reference to the edge data
   */
  template <typename EdgeIndex>
  PropertyReferenceType<EdgeIndex> GetEdgeData(const Edge& edge) {
    constexpr size_t prop_col_index = find_trait<EdgeIndex, EdgeProps>();

    if constexpr (katana::is_detected_v<has_undirected_t, PGView>) {
      return std::get<prop_col_index>(edge_view_)
          .GetValue(PGView::GetEdgePropertyIndexFromUndirectedEdge(edge));
    } else {
      return std::get<prop_col_index>(edge_view_)
          .GetValue(PGView::GetEdgePropertyIndexFromOutEdge(edge));
    }
  }

  /**
   * Gets the edge data.
   *
   * @param edge edge iterator to get the data of
   * @returns const reference to the edge data
   */
  template <typename EdgeIndex>
  PropertyConstReferenceType<EdgeIndex> GetEdgeData(const Edge& edge) const {
    constexpr size_t prop_col_index = find_trait<EdgeIndex, EdgeProps>();

    if constexpr (katana::is_detected_v<has_undirected_t, PGView>) {
      return std::get<prop_col_index>(edge_view_)
          .GetValue(PGView::GetEdgePropertyIndexFromUndirectedEdge(edge));
    } else {
      return std::get<prop_col_index>(edge_view_)
          .GetValue(PGView::GetEdgePropertyIndexFromOutEdge(edge));
    }
  }

  static Result<TypedPropertyGraphView<PGView, NodeProps, EdgeProps>> Make(
      PropertyGraph* pg, const std::vector<std::string>& node_properties,
      const std::vector<std::string>& edge_properties);
  static Result<TypedPropertyGraphView<PGView, NodeProps, EdgeProps>> Make(
      PropertyGraph* pg);
  static Result<TypedPropertyGraphView<PGView, NodeProps, EdgeProps>> Make(
      const PGView& pg_view, const std::vector<std::string>& node_properties,
      const std::vector<std::string>& edge_properties);
  static Result<TypedPropertyGraphView<PGView, NodeProps, EdgeProps>> Make(
      const PGView& pg_view);
};

/**
   * Finds a node in the sorted edgelist of some other node using binary search.
   *
   * @param graph graph to search in the topology of
   * @param node node to find in the edgelist of
   * @param node_to_find node id of the node to find in the edgelist of "node"
   * @returns iterator to the edge with id "node_to_find" if present else return "end" iterator
   */
template <typename GraphTy>
KATANA_EXPORT typename GraphTy::edge_iterator
FindEdgeSortedByDest(
    const GraphTy& graph, typename GraphTy::Node node,
    typename GraphTy::Node node_to_find) {
  auto edge_matched = katana::FindEdgeSortedByDest(
      &graph.GetPropertyGraph(), node, node_to_find);
  return typename GraphTy::edge_iterator(edge_matched);
}

template <typename NodeProps, typename EdgeProps>
Result<TypedPropertyGraph<NodeProps, EdgeProps>>
TypedPropertyGraph<NodeProps, EdgeProps>::Make(
    PropertyGraph* pg, const std::vector<std::string>& node_properties,
    const std::vector<std::string>& edge_properties) {
  auto node_view_result =
      internal::MakeNodePropertyViews<NodeProps>(pg, node_properties);
  if (!node_view_result) {
    return node_view_result.error();
  }

  auto edge_view_result =
      internal::MakeEdgePropertyViews<EdgeProps>(pg, edge_properties);
  if (!edge_view_result) {
    return edge_view_result.error();
  }

  return TypedPropertyGraph(
      pg, std::move(node_view_result.value()),
      std::move(edge_view_result.value()));
}

template <typename NodeProps, typename EdgeProps>
Result<TypedPropertyGraph<NodeProps, EdgeProps>>
TypedPropertyGraph<NodeProps, EdgeProps>::Make(PropertyGraph* pg) {
  return TypedPropertyGraph<NodeProps, EdgeProps>::Make(
      pg, pg->loaded_node_schema()->field_names(),
      pg->loaded_edge_schema()->field_names());
}

template <typename PGView, typename NodeProps, typename EdgeProps>
Result<TypedPropertyGraphView<PGView, NodeProps, EdgeProps>>
TypedPropertyGraphView<PGView, NodeProps, EdgeProps>::Make(
    PropertyGraph* pg, const std::vector<std::string>& node_properties,
    const std::vector<std::string>& edge_properties) {
  auto pg_view = pg->BuildView<PGView>();
  KATANA_LOG_DEBUG_ASSERT(pg);
  auto node_view_result =
      internal::MakeNodePropertyViews<NodeProps>(pg, node_properties);
  if (!node_view_result) {
    return node_view_result.error();
  }

  auto edge_view_result =
      internal::MakeEdgePropertyViews<EdgeProps>(pg, edge_properties);
  if (!edge_view_result) {
    return edge_view_result.error();
  }

  return TypedPropertyGraphView(
      pg_view, std::move(node_view_result.value()),
      std::move(edge_view_result.value()));
}

template <typename PGView, typename NodeProps, typename EdgeProps>
Result<TypedPropertyGraphView<PGView, NodeProps, EdgeProps>>
TypedPropertyGraphView<PGView, NodeProps, EdgeProps>::Make(PropertyGraph* pg) {
  auto pg_view = pg->BuildView<PGView>();
  return TypedPropertyGraphView<PGView, NodeProps, EdgeProps>::Make(
      pg_view, pg->loaded_node_schema()->field_names(),
      pg->loaded_edge_schema()->field_names());
}

template <typename PGView, typename NodeProps, typename EdgeProps>
Result<TypedPropertyGraphView<PGView, NodeProps, EdgeProps>>
TypedPropertyGraphView<PGView, NodeProps, EdgeProps>::Make(
    const PGView& pg_view, const std::vector<std::string>& node_properties,
    const std::vector<std::string>& edge_properties) {
  const auto* pg = pg_view.property_graph();
  auto node_view_result =
      internal::MakeNodePropertyViews<NodeProps>(pg, node_properties);
  if (!node_view_result) {
    return node_view_result.error();
  }

  auto edge_view_result =
      internal::MakeEdgePropertyViews<EdgeProps>(pg, edge_properties);
  if (!edge_view_result) {
    return edge_view_result.error();
  }

  return TypedPropertyGraphView(
      pg_view, std::move(node_view_result.value()),
      std::move(edge_view_result.value()));
}

template <typename PGView, typename NodeProps, typename EdgeProps>
Result<TypedPropertyGraphView<PGView, NodeProps, EdgeProps>>
TypedPropertyGraphView<PGView, NodeProps, EdgeProps>::Make(
    const PGView& pg_view) {
  auto pg = pg_view.get_property_graph();
  return TypedPropertyGraphView<PGView, NodeProps, EdgeProps>::Make(
      pg_view, pg->loaded_node_schema()->field_names(),
      pg->loaded_edge_schema()->field_names());
}
}  // namespace katana

#endif
