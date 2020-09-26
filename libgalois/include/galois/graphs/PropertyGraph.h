#ifndef GALOIS_LIBGALOIS_GALOIS_GRAPHS_PROPERTYGRAPH_H_
#define GALOIS_LIBGALOIS_GALOIS_GRAPHS_PROPERTYGRAPH_H_

#include <tuple>

#include <arrow/type_fwd.h>
#include <boost/iterator/counting_iterator.hpp>

#include "galois/NoDerefIterator.h"
#include "galois/Result.h"
#include "galois/Traits.h"
#include "galois/graphs/Details.h"
#include "galois/graphs/PropertyFileGraph.h"

namespace galois::graphs {

/// A property graph is a graph that has properties associated with its nodes
/// and edges. A property has a name and value. Its value may be a primitive
/// type, a list of values or a composition of properties.
///
/// A PropertyGraph is a representation of a property graph that imposes a
/// typed view on top of an underlying \ref PropertyFileGraph. A
/// PropertyFileGraph is appropriate for cases where the graph is largely
/// uninterpreted and can be manipulated as a collection of bits. A
/// PropertyGraph is appropriate for cases where computation needs to be done
/// on the properties themselves.
///
/// \tparam NodeProps A tuple of property types (\ref Properties.h) for nodes
/// \tparam EdgeProps A tuple of property types for edges
template <typename NodeProps, typename EdgeProps>
class PropertyGraph {
  using NodeView = PropertyViewTuple<NodeProps>;
  using EdgeView = PropertyViewTuple<EdgeProps>;

  PropertyFileGraph* pfg_;

  NodeView node_view_;
  EdgeView edge_view_;

  PropertyGraph(PropertyFileGraph* pfg, NodeView node_view, EdgeView edge_view)
      : pfg_(pfg),
        node_view_(std::move(node_view)),
        edge_view_(std::move(edge_view)) {}

public:
  using node_properties = NodeProps;
  using edge_properties = EdgeProps;
  using node_iterator = boost::counting_iterator<uint32_t>;
  using edge_iterator = boost::counting_iterator<uint64_t>;
  using edges_iterator = StandardRange<NoDerefIterator<edge_iterator>>;
  using iterator = node_iterator;
  using Node = uint32_t;

  // Standard container concepts

  node_iterator begin() const { return node_iterator(0); }

  node_iterator end() const { return node_iterator(num_nodes()); }

  size_t size() const { return num_nodes(); }

  bool empty() const { return num_nodes() == 0; }

  // Graph accessors

  /// GetData returns the data for a node.
  template <typename NodeIndex>
  PropertyReferenceType<NodeIndex> GetData(const Node& node) {
    constexpr size_t prop_index = find_trait<NodeIndex, NodeProps>();
    return std::get<prop_index>(node_view_).GetValue(node);
  }
  /// GetData returns the data for a node.
  template <typename NodeIndex>
  PropertyReferenceType<NodeIndex> GetData(const node_iterator& node) {
    return GetData<NodeIndex>(*node);
  }

  /// GetData returns the data for an edge.
  template <typename EdgeIndex>
  PropertyReferenceType<EdgeIndex> GetEdgeData(const edge_iterator& edge) {
    constexpr size_t prop_index = find_trait<EdgeIndex, EdgeProps>();
    return std::get<prop_index>(edge_view_).GetValue(*edge);
  }

  node_iterator GetEdgeDest(const edge_iterator& edge) {
    auto node_id = pfg_->topology().out_dests->Value(*edge);
    return node_iterator(node_id);
  }

  uint64_t num_nodes() const { return pfg_->topology().num_nodes(); }
  uint64_t num_edges() const { return pfg_->topology().num_edges(); }

  /**
   * Gets the edge range of some node.
   *
   * @param node node to get the edge range of
   * @returns iterator to edges of node
   */
  edges_iterator edges(const node_iterator& node) const {
    auto [begin_edge, end_edge] = pfg_->topology().edge_range(*node);
    return internal::make_no_deref_range(
        edge_iterator(begin_edge), edge_iterator(end_edge));
  }

  /**
   * Gets the first edge of some node.
   *
   * @param node node to get the edge of
   * @returns iterator to first edge of node
   */
  edge_iterator edge_begin(Node node) const { return *edges(node).begin(); }

  /**
   * Gets the end edge boundary of some node.
   *
   * @param node node to get the edge of
   * @returns iterator to the end of the edges of node, i.e. the first edge of
   *     the next node (or an "end" iterator if there is no next node)
   */
  edge_iterator edge_end(Node node) const { return *edges(node).end(); }

  // Graph constructors
  static Result<PropertyGraph<NodeProps, EdgeProps>> Make(
      PropertyFileGraph* pfg);
};

template <typename NodeProps, typename EdgeProps>
Result<PropertyGraph<NodeProps, EdgeProps>>
PropertyGraph<NodeProps, EdgeProps>::Make(PropertyFileGraph* pfg) {
  auto node_view_result = pfg->MakeNodePropertyViews<NodeProps>();
  if (!node_view_result) {
    return node_view_result.error();
  }

  auto edge_view_result = pfg->MakeEdgePropertyViews<EdgeProps>();
  if (!edge_view_result) {
    return edge_view_result.error();
  }

  return PropertyGraph(
      pfg, std::move(node_view_result.value()),
      std::move(edge_view_result.value()));
}
}  // namespace galois::graphs

#endif
