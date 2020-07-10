#include "galois/OutIndexView.h"
#include "galois/Result.h"
#include "tsuba/file.h"

namespace galois {

galois::Result<void> OutIndexView::Bind() {
  struct GRHeader header;
  if (auto res = tsuba::FilePeek(filename_, &header); !res) {
    return res.error();
  }
  if (auto res = file_.Bind(filename_, sizeof(header) +
                                           header.num_nodes_ * sizeof(index_t));
      !res) {
    return res.error();
  }
  gr_view_ = file_.ptr<GRPrefix>();
  return galois::ResultSuccess();
}

galois::Result<void> OutIndexView::Unbind() {
  galois::Result<void> res = galois::ResultSuccess();
  if (res = file_.Unbind(); !res) {
    GALOIS_LOG_ERROR("Unbind: {}", res.error());
  }
  gr_view_ = nullptr;
  return res;
}

} /* namespace galois */
