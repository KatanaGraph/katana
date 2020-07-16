#ifndef GALOIS_LIBGALOIS_GALOIS_PARTIAL_GRAPH_VIEW_H_
#define GALOIS_LIBGALOIS_GALOIS_PARTIAL_GRAPH_VIEW_H_

#include <fcntl.h>
#include <cstdio>
#include <memory>

#include <boost/iterator/counting_iterator.hpp>

#include "galois/OutIndexView.h"
#include "galois/graphs/GraphHelpers.h"
#include "galois/Result.h"
#include "tsuba/FileView.h"
#include "tsuba/RDG.h"

namespace galois {

template <typename Edge>
class PartialGraphView {
  tsuba::RDG rdg_;
  const Edge* edges_;
  uint64_t first_node_;
  uint64_t last_node_;
  uint64_t first_edge_;
  uint64_t last_edge_;

  PartialGraphView(tsuba::RDG&& rdg, uint64_t first_node, uint64_t last_node,
                   uint64_t first_edge, uint64_t last_edge)
      : rdg_(std::move(rdg)), edges_(rdg_.topology_file_storage.ptr<Edge>()),
        first_node_(first_node), last_node_(last_node), first_edge_(first_edge),
        last_edge_(last_edge) {}

public:
  typedef boost::counting_iterator<uint64_t> edge_iterator;
  typedef boost::counting_iterator<uint64_t> iterator;

  static galois::Result<PartialGraphView>
  Make(tsuba::RDGHandle handle, const OutIndexView& oiv, uint64_t first_node,
       uint64_t last_node, const std::vector<std::string>& node_properties,
       const std::vector<std::string>& edge_properties) {

    uint64_t first_edge   = *oiv.edge_begin(first_node);
    uint64_t last_edge    = *oiv.edge_begin(last_node);
    uint64_t edges_offset = oiv.view_size();
    uint64_t edges_start  = edges_offset + (first_edge * sizeof(Edge));
    uint64_t edges_stop   = edges_offset + (last_edge * sizeof(Edge));
    auto rdg_res          = tsuba::LoadPartial(
        handle, std::make_pair(first_node, last_node),
        std::make_pair(first_edge, last_edge), edges_start,
        edges_stop - edges_start, node_properties, edge_properties);
    if (!rdg_res) {
      return rdg_res.error();
    }
    return PartialGraphView(std::move(rdg_res.value()), first_node, last_node,
                            first_edge, last_edge);
  }

  iterator node_begin() const { return iterator(first_node_); }
  iterator node_end() const { return iterator(last_node_); }
  edge_iterator edge_begin() const { return edge_iterator(first_edge_); }
  edge_iterator edge_end() const { return edge_iterator(last_edge_); }
  const Edge* edges() const { return edges_; }
  const tsuba::RDG& prdg() const { return rdg_; }
};

} /* namespace galois */

#endif
