#include <time.h>

#include "bench_utils.h"
#include "galois/FileSystem.h"
#include "galois/Logging.h"
#include "tsuba/FileFrame.h"
#include "tsuba/FileView.h"
#include "tsuba/RDG.h"
#include "tsuba/file.h"
#include "tsuba/tsuba.h"

/// Test to make sure we can copy different property graph inputs to S3
/// locations.
///  After the copy, read the graph back and make sure it matches our in-memory
///  version.

std::vector<std::string> s3_pg_inputs = {
    "s3://non-property-graphs/rmat15",
    //    "s3://property-graphs/katana/yago-schema/meta",
    //    "s3://property-graphs/katana/ldbc_003/meta",
    //    "s3://property-graphs/katana/yago-shapes/meta",
};

std::vector<std::string> s3_pg_outputs = {
    "s3://katana-ci/delete_me/rmat15",
    //    "s3://katana-ci/delete_me/katana/yago-schema/meta",
    //    "s3://katana-ci/delete_me/katana/ldbc_003/meta",
    //    "s3://katana-ci/delete_me/katana/yago-shapes/meta",
};

void
VerifyCopy(const tsuba::RDG& s3_rdg, const std::string& s3_pg_out) {
  auto new_local_handle_res = tsuba::Open(s3_pg_out, tsuba::kReadOnly);
  if (!new_local_handle_res) {
    GALOIS_LOG_FATAL("Open new local rdg: {}", new_local_handle_res.error());
  }
  auto new_local_handle = new_local_handle_res.value();

  auto new_rdg_res = tsuba::RDG::Load(new_local_handle);
  GALOIS_LOG_ASSERT(new_rdg_res);

  auto new_rdg = std::move(new_rdg_res.value());
  GALOIS_LOG_ASSERT(new_rdg.Equals(s3_rdg));
}

galois::Result<tsuba::RDG>
DoCopy(const std::string& s3_pg_in, const std::string& s3_pg_out) {
  auto in_handle_res = tsuba::Open(s3_pg_in, tsuba::kReadOnly);
  if (!in_handle_res) {
    GALOIS_LOG_FATAL("Open rdg: {}", in_handle_res.error());
  }
  auto in_handle = in_handle_res.value();

  auto in_rdg_res = tsuba::RDG::Load(in_handle);
  if (!in_rdg_res) {
    GALOIS_LOG_FATAL("Load rdg from s3: {}", in_rdg_res.error());
  }
  auto in_rdg = std::move(in_rdg_res.value());

  if (auto res = tsuba::Create(s3_pg_out); !res) {
    GALOIS_LOG_FATAL("create rdg: {}", res.error());
  }

  auto out_handle_res = tsuba::Open(s3_pg_out, tsuba::kReadWrite);
  if (!out_handle_res) {
    GALOIS_LOG_FATAL("Open local rdg: {}", out_handle_res.error());
  }
  auto out_handle = out_handle_res.value();

  if (auto res = in_rdg.Store(out_handle); !res) {
    GALOIS_LOG_FATAL("Store local rdg: {}", res.error());
  }

  if (auto res = tsuba::Close(out_handle); !res) {
    GALOIS_LOG_FATAL("Close out handle: {}", res.error());
  }
  return tsuba::RDG(std::move(in_rdg));
}

void
CopyVerify(const std::string& s3_pg_in, const std::string& s3_pg_out) {
  std::vector<int64_t> results;
  struct timespec start = now();
  auto rdg_res = DoCopy(s3_pg_in, s3_pg_out);
  results.push_back(timespec_to_us(timespec_sub(now(), start)));
  if (!rdg_res) {
    GALOIS_LOG_FATAL("Copy failed: {}", rdg_res.error());
  }
  fmt::print("  Copy       : {}\n", FmtResults(results));
  tsuba::RDG s3_rdg(std::move(rdg_res.value()));

  results.clear();
  start = now();
  VerifyCopy(s3_rdg, s3_pg_out);
  results.push_back(timespec_to_us(timespec_sub(now(), start)));
  fmt::print("  Equal check: {}\n", FmtResults(results));
}

int
main() {
  if (auto init_good = tsuba::Init(); !init_good) {
    GALOIS_LOG_FATAL("tsuba::Init: {}", init_good.error());
  }

  GALOIS_LOG_ASSERT(s3_pg_inputs.size() == s3_pg_outputs.size());
  for (auto i = 0U; i < s3_pg_inputs.size(); ++i) {
    fmt::print("Copy {}\n  to {}\n", s3_pg_inputs[i], s3_pg_outputs[i]);
    CopyVerify(s3_pg_inputs[i], s3_pg_outputs[i]);
  }

  return 0;
}
