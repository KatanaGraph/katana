#ifndef GALOIS_LIBGALOIS_GALOIS_OUTINDEXVIEW_H_
#define GALOIS_LIBGALOIS_GALOIS_OUTINDEXVIEW_H_

#include <string>
#include <cstdint>

#include <boost/iterator/counting_iterator.hpp>

#include "galois/config.h"
#include "galois/graphs/GraphHelpers.h"
#include "tsuba/FileView.h"
#include "tsuba/RDG.h"
#include "tsuba/Errors.h"

namespace galois {

class GALOIS_EXPORT OutIndexView {
  tsuba::RDGPrefix pfx_;

  OutIndexView(tsuba::RDGPrefix&& pfx) noexcept : pfx_(std::move(pfx)) {}

public:
  OutIndexView(const OutIndexView&) = delete;
  OutIndexView& operator=(const OutIndexView&) = delete;

  OutIndexView(OutIndexView&& other) = default;
  OutIndexView& operator=(OutIndexView&& other) = default;

  static galois::Result<OutIndexView> Make(tsuba::RDGHandle handle);
  static galois::Result<OutIndexView> Make(const std::string& path);

  uint64_t num_nodes() const { return pfx_.prefix->header.num_nodes; }
  uint64_t num_edges() const { return pfx_.prefix->header.num_edges; }
  uint64_t view_offset() const { return pfx_.view_offset; }

  const uint64_t& operator[](uint64_t n) const {
    assert(n < pfx_.prefix->header.num_nodes);
    return pfx_.prefix->out_indexes[n];
  }

  const tsuba::GRPrefix* gr_view() const { return pfx_.prefix; }
};
} // namespace galois

#endif
