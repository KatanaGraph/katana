#include <time.h>

#include "galois/Logging.h"
#include "galois/FileSystem.h"
#include "tsuba/tsuba.h"
#include "tsuba/FileFrame.h"
#include "tsuba/FileView.h"
#include "tsuba/file.h"
#include "tsuba/RDG.h"

// static uint8_t data_100[100];
std::vector<std::string> s3_pg_inputs = {
//  "s3://katana-ci/yago-shapes/meta",
    "s3://property-graphs/katana/yago-schema/meta",
    //  "s3://property-graphs/katana/ldbc_003/meta",
    //  "s3://property-graphs/katana/yago-shapes/meta",
};

std::vector<std::string> s3_pg_outputs = {
//  "s3://witchel-tests-east2/katana-ci/yago-shapes/meta",
   "s3://witchel-tests-east2/katana/yago-schema/meta",
  //  "s3://witchel-tests-east2/katana/ldbc_003/meta",
  //  "s3://witchel-tests-east2/katana/yago-shapes/meta",
};

void CopyGraph(const std::string& s3_pg_in, const std::string& s3_pg_out) {
  auto s3_handle_res = tsuba::Open(s3_pg_in, tsuba::kReadOnly);
  if (!s3_handle_res) {
    GALOIS_LOG_FATAL("Open rdg: {}", s3_handle_res.error());
  }
  auto s3_handle = s3_handle_res.value();

  auto s3_rdg_res = tsuba::Load(s3_handle);
  if (!s3_rdg_res) {
    GALOIS_LOG_FATAL("Load rdg from s3: {}", s3_rdg_res.error());
  }
  auto s3_rdg = std::move(s3_rdg_res.value());

  if (auto res = tsuba::Create(s3_pg_out); !res) {
    GALOIS_LOG_FATAL("create rdg: {}", res.error());
  }

  auto local_handle_res = tsuba::Open(s3_pg_out, tsuba::kReadWrite);
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
}


int main() {
  if (auto init_good = tsuba::Init(); !init_good) {
    GALOIS_LOG_FATAL("tsuba::Init: {}", init_good.error());
  }

  // auto unique_result = galois::CreateUniqueDirectory(s3_url_base);
  // GALOIS_LOG_ASSERT(unique_result);
  // std::string temp_dir(std::move(unique_result.value()));
  // fmt::print("RRRR {}\n", temp_dir);

  // arrow_file(data_100, sizeof(data_100), temp_dir);

  GALOIS_LOG_ASSERT(s3_pg_inputs.size() == s3_pg_outputs.size());
  for(auto i = 0U; i < s3_pg_inputs.size(); ++i) {
    GALOIS_LOG_VERBOSE("Copy {} to {}\n", s3_pg_inputs[i], s3_pg_outputs[i]);
    CopyGraph(s3_pg_inputs[i], s3_pg_outputs[i]);
  }

  return 0;
}
