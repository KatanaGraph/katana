#ifndef GALOIS_LIBGALOIS_GALOIS_PARTIALGRAPHVIEW_H_
#define GALOIS_LIBGALOIS_GALOIS_PARTIALGRAPHVIEW_H_

#include <memory>
#include <utility>

#include <boost/iterator/counting_iterator.hpp>

#include "galois/OutIndexView.h"
#include "galois/Range.h"
#include "galois/Result.h"
#include "tsuba/RDG.h"

namespace galois {

/// A PartialGraphView is a view of a graph constrained to a contiguous range
/// of nodes and constrained to a specific set of node and edge properties.
template <typename Edge>
class PartialGraphView {
  tsuba::RDG rdg_;
  galois::OutIndexView view_;

  const tsuba::GRPrefix* prefix_;
  const Edge* edges_;

  std::pair<uint64_t, uint64_t> node_range_;
  std::pair<uint64_t, uint64_t> edge_range_;

  PartialGraphView(tsuba::RDG&& rdg, galois::OutIndexView&& view,
                   std::pair<uint64_t, uint64_t> node_range,
                   std::pair<uint64_t, uint64_t> edge_range)
      : rdg_(std::move(rdg)), view_(std::move(view)), prefix_(view_.gr_view()),
        edges_(rdg_.topology_file_storage_.valid_ptr<Edge>()),
        node_range_(std::move(node_range)), edge_range_(std::move(edge_range)) {
  }

  static uint64_t EdgeBegin(const tsuba::GRPrefix* prefix, uint64_t node_id) {
    if (node_id == 0) {
      return 0;
    }
    return prefix->out_indexes[node_id - 1];
  }

  static uint64_t EdgeEnd(const tsuba::GRPrefix* prefix, uint64_t node_id) {
    return prefix->out_indexes[node_id];
  }

  static tsuba::RDG::SliceArg BuildSliceArg(const OutIndexView& oiv,
                                            uint64_t first_node,
                                            uint64_t last_node) {
    const tsuba::GRPrefix* prefix = oiv.gr_view();

    uint64_t first_edge   = EdgeBegin(prefix, first_node);
    uint64_t last_edge    = EdgeBegin(prefix, last_node);
    uint64_t edges_offset = oiv.view_offset();
    uint64_t edges_start  = edges_offset + (first_edge * sizeof(Edge));
    uint64_t edges_stop   = edges_offset + (last_edge * sizeof(Edge));

    return tsuba::RDG::SliceArg{
        .node_range = std::make_pair(first_node, last_node),
        .edge_range = std::make_pair(first_edge, last_edge),
        .topo_off   = edges_start,
        .topo_size  = edges_stop - edges_start,
    };
  }

public:
  typedef StandardRange<boost::counting_iterator<uint64_t>> edges_iterator;
  typedef StandardRange<boost::counting_iterator<uint64_t>> nodes_iterator;

  /// Make a partial graph view from a partially loaded RDG, as indicated by a
  /// RDGHandle and OutIndexView, which loads only the specified properties
  static galois::Result<PartialGraphView>
  Make(tsuba::RDGHandle handle, OutIndexView&& oiv, uint64_t first_node,
       uint64_t last_node,
       const std::vector<std::string>* node_properties = nullptr,
       const std::vector<std::string>* edge_properties = nullptr) {

    auto view                  = std::move(oiv);
    tsuba::RDG::SliceArg slice = BuildSliceArg(view, first_node, last_node);
    auto rdg_res = tsuba::RDG::LoadPartial(handle, slice, node_properties,
                                           edge_properties);
    if (!rdg_res) {
      return rdg_res.error();
    }
    return PartialGraphView(std::move(rdg_res.value()), std::move(view),
                            slice.node_range, slice.edge_range);
  }

  /// Make a partial graph view from a partially loaded RDG, as indicated by a
  /// RDGHandle and OutIndexView, which loads only the specified properties
  static galois::Result<PartialGraphView>
  Make(const std::string& uri, OutIndexView&& oiv, uint64_t first_node,
       uint64_t last_node,
       const std::vector<std::string>* node_properties = nullptr,
       const std::vector<std::string>* edge_properties = nullptr) {

    auto view                  = std::move(oiv);
    tsuba::RDG::SliceArg slice = BuildSliceArg(view, first_node, last_node);
    auto rdg_res =
        tsuba::RDG::LoadPartial(uri, slice, node_properties, edge_properties);
    if (!rdg_res) {
      return rdg_res.error();
    }
    return PartialGraphView(std::move(rdg_res.value()), std::move(view),
                            slice.node_range, slice.edge_range);
  }

  nodes_iterator nodes() const {
    return MakeStandardRange(
        boost::counting_iterator<uint64_t>(node_range_.first),
        boost::counting_iterator<uint64_t>(node_range_.second));
  }

  edges_iterator all_edges() const {
    return MakeStandardRange(
        boost::counting_iterator<uint64_t>(edge_range_.first),
        boost::counting_iterator<uint64_t>(edge_range_.second));
  }

  edges_iterator edges(uint64_t node_id) const {
    return MakeStandardRange(
        boost::counting_iterator<uint64_t>(EdgeBegin(prefix_, node_id)),
        boost::counting_iterator<uint64_t>(EdgeEnd(prefix_, node_id)));
  }

  uint64_t GetEdgeDest(uint64_t edge_id) const {
    return edges_[GetEdgeOffset(edge_id)];
  }

  /// GetNodeOffset returns the offset into this PartialGraphView given a
  /// global node ID.
  uint64_t GetNodeOffset(uint64_t node_id) const {
    assert(node_range_.first <= node_id && node_id < node_range_.second);
    return node_id - node_range_.first;
  }

  /// GetEdgeOffset returns the offset into this PartialGraphView given a
  /// global edge ID.
  uint64_t GetEdgeOffset(uint64_t edge_id) const {
    assert(edge_range_.first <= edge_id && edge_id < edge_range_.second);
    return edge_id - edge_range_.first;
  }

  const tsuba::RDG& prdg() const { return rdg_; }
};

using PartialV1GraphView = PartialGraphView<uint32_t>;

} /* namespace galois */

#endif
