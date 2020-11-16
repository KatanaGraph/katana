#ifndef GALOIS_LIBGALOIS_GALOIS_PARTIALGRAPHVIEW_H_
#define GALOIS_LIBGALOIS_GALOIS_PARTIALGRAPHVIEW_H_

#include <memory>
#include <utility>

#include <boost/iterator/counting_iterator.hpp>

#include "galois/Range.h"
#include "galois/Result.h"
#include "tsuba/RDGPrefix.h"
#include "tsuba/RDGSlice.h"

namespace galois {

/// A PartialGraphView is a view of a graph constrained to a contiguous range
/// of nodes and constrained to a specific set of node and edge properties.
template <typename Edge>
class PartialGraphView {
  tsuba::RDGSlice rdg_slice_;

  const std::vector<uint64_t> out_indexes_;
  const Edge* edges_;

  std::pair<uint64_t, uint64_t> node_range_;
  std::pair<uint64_t, uint64_t> edge_range_;

  PartialGraphView(
      tsuba::RDGSlice&& rdg_slice, std::vector<uint64_t>&& out_indexes,
      std::pair<uint64_t, uint64_t> node_range,
      std::pair<uint64_t, uint64_t> edge_range)
      : rdg_slice_(std::move(rdg_slice)),
        out_indexes_(std::move(out_indexes)),
        edges_(rdg_slice_.topology_file_storage().template valid_ptr<Edge>()),
        node_range_(std::move(node_range)),
        edge_range_(std::move(edge_range)) {}

  static uint64_t EdgeBegin(const tsuba::RDGPrefix& pfx, uint64_t node_id) {
    if (node_id == 0) {
      return 0;
    }
    return pfx[node_id - 1];
  }

  static uint64_t EdgeEnd(const tsuba::RDGPrefix& pfx, uint64_t node_id) {
    return pfx[node_id];
  }

  static tsuba::RDGSlice::SliceArg BuildSliceArg(
      const tsuba::RDGPrefix& pfx, uint64_t first_node, uint64_t last_node) {
    uint64_t first_edge = EdgeBegin(pfx, first_node);
    uint64_t last_edge = EdgeBegin(pfx, last_node);
    uint64_t edges_offset = pfx.view_offset();
    uint64_t edges_start = edges_offset + (first_edge * sizeof(Edge));
    uint64_t edges_stop = edges_offset + (last_edge * sizeof(Edge));

    return tsuba::RDGSlice::SliceArg{
        .node_range = std::make_pair(first_node, last_node),
        .edge_range = std::make_pair(first_edge, last_edge),
        .topo_off = edges_start,
        .topo_size = edges_stop - edges_start,
    };
  }

  static std::vector<uint64_t> BuildOutIndexesSlice(
      const tsuba::RDGPrefix& pfx, std::pair<uint64_t, uint64_t> node_range) {
    return pfx.range(node_range.first, node_range.second);
  }

public:
  typedef StandardRange<boost::counting_iterator<uint64_t>> edges_iterator;
  typedef StandardRange<boost::counting_iterator<uint64_t>> nodes_iterator;

  /// Make a partial graph view from a partially loaded RDG, as indicated by a
  /// RDGHandle and OutIndexView, which loads only the specified properties
  static galois::Result<PartialGraphView> Make(
      tsuba::RDGHandle handle, tsuba::RDGPrefix&& pfx, uint64_t first_node,
      uint64_t last_node,
      const std::vector<std::string>* node_properties = nullptr,
      const std::vector<std::string>* edge_properties = nullptr) {
    auto view = std::move(pfx);
    tsuba::RDGSlice::SliceArg slice =
        BuildSliceArg(view, first_node, last_node);
    auto rdg_res =
        tsuba::RDGSlice::Load(handle, slice, node_properties, edge_properties);
    if (!rdg_res) {
      return rdg_res.error();
    }
    auto out_indexes = BuildOutIndexesSlice(view, slice.node_range);
    return PartialGraphView(
        std::move(rdg_res.value()), std::move(out_indexes), slice.node_range,
        slice.edge_range);
  }

  /// Make a partial graph view from a partially loaded RDG, as indicated by a
  /// RDGHandle and OutIndexView, which loads only the specified properties
  static galois::Result<PartialGraphView> Make(
      const std::string& uri, tsuba::RDGPrefix&& pfx, uint64_t first_node,
      uint64_t last_node,
      const std::vector<std::string>* node_properties = nullptr,
      const std::vector<std::string>* edge_properties = nullptr) {
    auto view = std::move(pfx);
    tsuba::RDGSlice::SliceArg slice =
        BuildSliceArg(view, first_node, last_node);
    auto rdg_res =
        tsuba::RDGSlice::Load(uri, slice, node_properties, edge_properties);
    if (!rdg_res) {
      return rdg_res.error();
    }
    auto out_indexes = BuildOutIndexesSlice(view, slice.node_range);
    return PartialGraphView(
        std::move(rdg_res.value()), std::move(out_indexes), slice.node_range,
        slice.edge_range);
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
    auto node_offset = GetNodeOffset(node_id);
    return MakeStandardRange(
        boost::counting_iterator<uint64_t>(
            (node_offset == 0) ? edge_range_.first
                               : out_indexes_[node_offset - 1]),
        boost::counting_iterator<uint64_t>(out_indexes_[node_offset]));
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

  const tsuba::RDGSlice& prdg() const { return rdg_slice_; }
};

using PartialV1GraphView = PartialGraphView<uint32_t>;

} /* namespace galois */

#endif
