#ifndef GALOIS_LIBGALOIS_GALOIS_OUT_INDEX_VIEW_H_
#define GALOIS_LIBGALOIS_GALOIS_OUT_INDEX_VIEW_H_

#include <string>
#include <cstdint>

#include <boost/iterator/counting_iterator.hpp>

#include "galois/graphs/GraphHelpers.h"
#include "tsuba/FileView.h"
#include "tsuba/RDG.h"
#include "tsuba/Errors.h"

namespace galois {

/* sizes in .gr files */
typedef uint32_t edge_v1_t;
typedef uint64_t edge_v2_t;

class OutIndexView {
  tsuba::RDGPrefix pfx_;

  OutIndexView(tsuba::RDGPrefix&& pfx) noexcept : pfx_(std::move(pfx)) {}

public:
  typedef boost::counting_iterator<uint64_t> edge_iterator;
  typedef boost::counting_iterator<uint64_t> iterator;

  OutIndexView(const OutIndexView&) = delete;
  OutIndexView& operator=(const OutIndexView&) = delete;

  OutIndexView(OutIndexView&& other) = default;
  OutIndexView& operator=(OutIndexView&& other) = default;

  static galois::Result<OutIndexView> Make(tsuba::RDGHandle handle);

  uint64_t num_nodes() const { return pfx_.prefix->header.num_nodes; }
  uint64_t num_edges() const { return pfx_.prefix->header.num_edges; }
  uint64_t view_size() const { return pfx_.prefix_storage.size(); }

  const uint64_t& operator[](uint64_t n) const {
    assert(n < pfx_.prefix->header.num_nodes);
    return pfx_.prefix->out_indexes[n];
  }
  edge_iterator edge_begin(uint64_t vertex) const {
    if (vertex == 0) {
      return edge_iterator(0);
    }
    return edge_iterator(this->operator[](vertex - 1));
  }
  const tsuba::GRPrefix* gr_view() const { return pfx_.prefix; }

  // typedefs used by divide by node below
  typedef std::pair<iterator, iterator> NodeRange;
  typedef std::pair<edge_iterator, edge_iterator> EdgeRange;
  typedef std::pair<NodeRange, EdgeRange> GraphRange;

  /**
   * Returns 2 ranges (one for nodes, one for edges) for a particular division.
   * The ranges specify the nodes/edges that a division is responsible for. The
   * function attempts to split them evenly among threads given some kind of
   * weighting
   *
   * @param nodeWeight weight to give to a node in division
   * @param edgeWeight weight to give to an edge in division
   * @param id Division number you want the ranges for
   * @param total Total number of divisions
   * @param scaleFactor Vector specifying if certain divisions should get more
   * than other divisions
   */
  auto DivideByNode(
      uint64_t nodeWeight, uint64_t edgeWeight, uint64_t id, uint64_t total,
      std::vector<unsigned> scaleFactor = std::vector<unsigned>()) const
      -> GraphRange {
    return galois::graphs::divideNodesBinarySearch(
        num_nodes(), num_edges(), nodeWeight, edgeWeight, id, total, *this,
        std::move(scaleFactor));
  }
};
} // namespace galois

#endif
