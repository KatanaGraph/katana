#include "galois/OutIndexView.h"
#include "galois/Result.h"

//#include <iostream>

//#include "galois/Logging.h"
#include "tsuba/RDG.h"
#include "tsuba/file.h"

namespace galois {

galois::Result<OutIndexView> OutIndexView::Make(tsuba::RDGHandle handle) {
  auto pfx_res = tsuba::ExaminePrefix(handle);
  if (!pfx_res) {
    return pfx_res.error();
  }
  return OutIndexView(std::move(pfx_res.value()));
}

} /* namespace galois */
