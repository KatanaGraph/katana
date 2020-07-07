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
typedef uint64_t index_t;
typedef uint32_t edge_v1_t;
typedef uint64_t edge_v2_t;

/* standard types */
typedef uint64_t sz_t; /* size_t can vary by platform (e.g., Native Client) */
typedef uint64_t vertex_t;

struct GRHeader {
  uint64_t version_;
  sz_t edge_type_size_;
  sz_t num_nodes_;
  sz_t num_edges_;
};

/* includes the header and the list of indexes */
struct GRPrefix {
  GRHeader header_;
  index_t out_indexes_[]; /* NOLINT length is defined by num_nodes_ in header */
};

class OutIndexView {
  // tsuba::FileView file_;
  std::string filename_;
  const GRPrefix* gr_view_;
  tsuba::RDG rdg_;

public:
  typedef boost::counting_iterator<uint64_t> edge_iterator;
  typedef boost::counting_iterator<uint64_t> iterator;

  OutIndexView(const OutIndexView&) = delete;
  OutIndexView& operator=(const OutIndexView&) = delete;

  // OutIndexView(std::string filename) : filename_(std::move(filename)) {}
  OutIndexView(OutIndexView&& other) = default;
  OutIndexView& operator=(OutIndexView&& other) = default;

  OutIndexView(tsuba::RDG&& rdg) : rdg_(std::move(rdg)) {}

  static Result<std::shared_ptr<OutIndexView>> Make(tsuba::RDG&& rdg);
  static Result<std::shared_ptr<OutIndexView>>
  Make(const std::string& metadata_path);
  static Result<std::shared_ptr<OutIndexView>>
  Make(const std::string& metadata_path,
       const std::vector<std::string>& node_properties,
       const std::vector<std::string>& edge_properties);

  ~OutIndexView() {
    if (auto res = Unbind(); !res) {
      GALOIS_LOG_ERROR("Unbind in destructor:", res.error());
    }
  }

  galois::Result<void> Bind();
  galois::Result<void> Unbind();

  const std::string& filename() const { return filename_; }
  uint64_t num_nodes() const { return gr_view_->header_.num_nodes_; }
  uint64_t num_edges() const { return gr_view_->header_.num_edges_; }
  // uint64_t view_size() const { return rdg_.topology_file_storage.size(); }
  uint64_t view_size() const {
    return sizeof(GRHeader) + gr_view_->header_.num_nodes_ * sizeof(index_t);
  }

  const uint64_t& operator[](uint64_t n) const {
    assert(n < gr_view_->header_.num_nodes_);
    return gr_view_->out_indexes_[n];
  }
  edge_iterator edge_begin(uint64_t vertex) const {
    if (vertex == 0) {
      return edge_iterator(0);
    }
    return edge_iterator(this->operator[](vertex - 1));
  }
  const GRPrefix* gr_view() const { return gr_view_; }
  const tsuba::RDG& get_rdg() const { return rdg_; };

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
