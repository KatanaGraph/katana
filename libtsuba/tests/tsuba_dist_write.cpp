#include <string>

#include <boost/filesystem.hpp>
#include <boost/filesystem/operations.hpp>

#include "MPICommBackend.h"
#include "galois/FileSystem.h"
#include "galois/Logging.h"
#include "tsuba/RDG.h"
#include "tsuba/tsuba.h"

namespace fs = boost::filesystem;

constexpr const char* local_file_dir = "/tmp/tsuba-dist-write-test";
constexpr const char* test_prop_graph = "s3://katana-ci/yago-shapes/meta";

void
DownloadGraph() {
  auto s3_handle_res = tsuba::Open(test_prop_graph, tsuba::kReadOnly);
  if (!s3_handle_res) {
    GALOIS_LOG_FATAL("Open rdg from s3: {}", s3_handle_res.error());
  }

  auto s3_handle = s3_handle_res.value();

  auto s3_rdg_res = tsuba::RDG::Load(s3_handle);
  if (!s3_rdg_res) {
    GALOIS_LOG_FATAL("Load rdg from s3: {}", s3_rdg_res.error());
  }
  auto s3_rdg = std::move(s3_rdg_res.value());

  // Create a new unique place in the local filesytem
  std::string meta_file{local_file_dir};
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

  if (auto res = s3_rdg.Store(local_handle); !res) {
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

  auto new_rdg_res = tsuba::RDG::Load(new_local_handle);
  GALOIS_LOG_ASSERT(new_rdg_res);

  auto new_rdg = std::move(new_rdg_res.value());
  GALOIS_LOG_ASSERT(new_rdg.Equals(s3_rdg));
}

int
main() {
  // fs::remove_all(local_file_dir);

  if (auto init_good = tsuba::InitWithMPI(); !init_good) {
    GALOIS_LOG_FATAL("tsuba::InitWithMPI: {}", init_good.error());
  }
  DownloadGraph();
  if (auto fini_good = tsuba::FiniWithMPI(); !fini_good) {
    GALOIS_LOG_FATAL("tsuba::FiniWithMPI: {}", fini_good.error());
  }
  return 0;
}
