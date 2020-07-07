#include "galois/OutIndexView.h"
#include "galois/Result.h"

//#include <iostream>
#include <boost/filesystem.hpp>

//#include "galois/Logging.h"
#include "tsuba/file.h"

namespace fs = boost::filesystem;
namespace galois {

galois::Result<void> OutIndexView::Bind() {
  // struct GRHeader header;
  // if (auto res = tsuba::FilePeek(filename_, &header); !res) {
  //   return res.error();
  // }
  // if (auto res = file_.Bind(filename_, sizeof(header) +
  //                                          header.num_nodes_ *
  //                                          sizeof(index_t));
  //     !res) {
  //   return res.error();
  // }
  // gr_view_ = file_.ptr<GRPrefix>();
  gr_view_ = rdg_.topology_file_storage.ptr<GRPrefix>();
  return galois::ResultSuccess();
}

galois::Result<void> OutIndexView::Unbind() {
  galois::Result<void> res = galois::ResultSuccess();
  // if (res = file_.Unbind(); !res) {
  //   GALOIS_LOG_ERROR("Unbind: {}", res.error());
  // }
  gr_view_ = nullptr;
  return res;
}

galois::Result<std::shared_ptr<galois::OutIndexView>>
MakeOutIndexView(tsuba::RDGHandle& handle,
                 const std::vector<std::string>& node_properties,
                 const std::vector<std::string>& edge_properties) {
  // Only reads the meta data for partial loading
  // auto rdg_result = tsuba::ReadMetadata(handle);
  auto rdg_result = tsuba::Load(handle, node_properties, edge_properties);

  if (!rdg_result) {
    return rdg_result.error();
  }
  // auto set_dir_result = SetRDGDir(&rdg_result.value(), handle);
  // GALOIS_LOG_ASSERT(set_dir_result);
  return galois::OutIndexView::Make(std::move(rdg_result.value()));
}

galois::Result<std::shared_ptr<galois::OutIndexView>>
galois::OutIndexView::Make(const std::string& metadata_path,
                           const std::vector<std::string>& node_properties,
                           const std::vector<std::string>& edge_properties) {
  auto handle = tsuba::Open(metadata_path, tsuba::kReadOnly);
  if (!handle) {
    return handle.error();
  }
  return MakeOutIndexView(handle.value(), node_properties, edge_properties);
}

galois::Result<std::shared_ptr<galois::OutIndexView>>
galois::OutIndexView::Make(const std::string& metadata_path) {
  auto handle = tsuba::Open(metadata_path, tsuba::kReadOnly);
  if (!handle) {
    return handle.error();
  }
  return MakeOutIndexView(handle.value(), std::move(std::vector<std::string>()),
                          std::move(std::vector<std::string>()));
}

galois::Result<std::shared_ptr<galois::OutIndexView>>
galois::OutIndexView::Make(tsuba::RDG&& rdg) {
  auto out_index_view = std::make_shared<OutIndexView>(std::move(rdg));
  fs::path t_path{out_index_view->rdg_.rdg_dir};
  t_path.append(out_index_view->rdg_.topology_path);
  out_index_view->filename_ = t_path.string();

  return out_index_view;
}

} /* namespace galois */
