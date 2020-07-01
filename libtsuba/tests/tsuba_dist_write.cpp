#include <string>

#include <boost/filesystem.hpp>

#include "galois/Logging.h"
#include "galois/FileSystem.h"
#include "tsuba/tsuba.h"
#include "tsuba/RDG.h"

namespace fs = boost::filesystem;

constexpr const char* test_prop_graph = "s3://katana-ci/yago-shapes/meta";

void DownloadGraph() {
  auto s3_handle_res = tsuba::Open(test_prop_graph, tsuba::kReadOnly);
  if (!s3_handle_res) {
    GALOIS_LOG_FATAL("Open rdg from s3: {}", s3_handle_res.error());
  }

  auto s3_handle = s3_handle_res.value();

  auto s3_rdg_res = tsuba::Load(s3_handle);
  if (!s3_rdg_res) {
    GALOIS_LOG_FATAL("Load rdg from s3: {}", s3_rdg_res.error());
  }
  auto s3_rdg = std::move(s3_rdg_res.value());

  /*
  // Forget that we have these files on disk so that we can write them out again
  if (auto res = UnbindFromStorage(&s3_rdg); !res) {
    GALOIS_LOG_FATAL("Unbind rdg: {}", res.error());
  }
  */

  // Create a new unique place in the local filesytem
  auto dir_res = galois::CreateUniqueDirectory("/tmp/thunt-");
  if (!dir_res) {
    GALOIS_LOG_FATAL("CreateUniqueDirectory: {}", dir_res.error());
  }
  auto dir = dir_res.value();

  std::string meta_file{dir};
  meta_file += "/test_graph";

  GALOIS_LOG_WARN("creating temp file {}", meta_file);

  if (auto res = tsuba::Create(meta_file); !res) {
    GALOIS_LOG_FATAL("create rdg: {}", res.error());
  }

  auto local_handle_res = tsuba::Open(meta_file, tsuba::kReadWrite);
  if (!local_handle_res) {
    GALOIS_LOG_FATAL("Open local rdg: {}", local_handle_res.error());
  }
  auto local_handle = local_handle_res.value();

  if (auto res = tsuba::Store(local_handle, &s3_rdg); !res) {
    GALOIS_LOG_FATAL("Store local rdg: {}", res.error());
  }

  if (auto res = tsuba::Close(local_handle); !res) {
    GALOIS_LOG_FATAL("Close local handle: {}", res.error());
  }

  auto new_local_handle_res = tsuba::Open(meta_file, tsuba::kReadOnly);
  if (!new_local_handle_res) {
    GALOIS_LOG_FATAL("Open new local rdg: {}", new_local_handle_res.error());
  }
  auto new_local_handle = new_local_handle_res.value();

  auto new_rdg_res = tsuba::Load(new_local_handle);
  GALOIS_LOG_ASSERT(new_rdg_res);

  auto new_rdg = std::move(new_rdg_res.value());
  GALOIS_LOG_ASSERT(new_rdg.Equals(s3_rdg));
}

int main() {
  if (auto init_good = tsuba::Init(); !init_good) {
    GALOIS_LOG_FATAL("tsuba::Init: {}", init_good.error());
  }
  DownloadGraph();
  return 0;
}
