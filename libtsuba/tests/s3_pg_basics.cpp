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
  auto handle_res = tsuba::Open(s3_pg_in, tsuba::kReadOnly);
  if (!handle_res) {
    GALOIS_LOG_FATAL("Open rdg: {}", handle_res.error());
  }
  auto rdg_res = tsuba::Load(handle_res.value());
  GALOIS_LOG_ASSERT(rdg_res);
  tsuba::RDG rdg = std::move(rdg_res.value());
  // std::shared_ptr<tsuba::RDGHandle> rdg =
  // std::make_shared<tsuba::FileView>(rdg_res.value());

  // NB: Current limitation of API requires Detach call
  if (auto res = rdg.Detach(); !res) {
    GALOIS_LOG_FATAL("Detach rdg: {}", res.error());
  }

  if (auto rename_res =
          tsuba::Rename(rdg.handle, s3_pg_out, tsuba::kReadWrite);
      !rename_res) {
    GALOIS_LOG_FATAL("Rename to {}: {}", s3_pg_out, rename_res.error());
  }
  if (auto store_res = tsuba::Store(rdg); !store_res) {
    GALOIS_LOG_FATAL("Store failed: {}", store_res.error());
  }
}

#if 0
static void arrow_file(uint8_t bits[], uint64_t num_bytes, std::string& dir) {
  auto handle_res = tsuba::Open("s3://katana-ci/yago-shapes/meta", tsuba::kReadOnly);
  if (!handle_res) {
    GALOIS_LOG_FATAL("Open rdg: {}", handle_res.error());
  }
  auto rdg_res = tsuba::Load(handle_res.value());
  GALOIS_LOG_ASSERT(rdg_res);
  auto rdg = std::move(rdg_res.value());
  if (auto res = rdg.Detach(); !res) {
    GALOIS_LOG_FATAL("Detach rdg: {}", res.error());
  }
  
  // Write
  std::string filename = dir + "arrow_file";
  auto ff              = tsuba::FileFrame();
  int err              = ff.Init();
  GALOIS_LOG_ASSERT(!err);

  arrow::Status aro_sts = ff.Write(bits, num_bytes);
  GALOIS_LOG_ASSERT(aro_sts.ok());
  ff.Bind(filename);
  err = ff.Persist();
  GALOIS_LOG_ASSERT(!err);

  // Validate
  tsuba::StatBuf buf;
  err = tsuba::FileStat(filename, &buf);
  GALOIS_LOG_ASSERT(!err);
  GALOIS_LOG_ASSERT(buf.size == num_bytes);

  // Read
  uint64_t res[num_bytes];
  auto fv = tsuba::FileView();
  err     = fv.Bind(filename);
  GALOIS_LOG_ASSERT(!err);
  arrow::Result<int64_t> aro_res = fv.Read(92, res);
  GALOIS_LOG_ASSERT(aro_res.ok());
  int64_t bytes_read = aro_res.ValueOrDie();
  GALOIS_LOG_ASSERT(bytes_read == 92);
  GALOIS_LOG_ASSERT(!memcmp(res, bits, 92));
}

// 21 chars, with 1 null byte
void get_time_string(char* buf, int limit) {
  time_t rawtime;
  struct tm* timeinfo;
  time(&rawtime);
  timeinfo = localtime(&rawtime);
  strftime(buf, limit, "%Y/%m/%d %H:%M:%S ", timeinfo);
}

void init_data(uint8_t* buf, int limit) {
  if (limit < 0)
    return;
  if (limit < 19) {
    for (; limit; limit--) {
      *buf++ = 'a';
    }
    return;
  } else {
    char tmp[32];             // Generous with space
    get_time_string(tmp, 31); // Trailing null
                              // Copy without trailing null
    memcpy(buf, tmp, 19);
    buf += 19;
    if (limit > 19) {
      *buf++ = ' ';
      for (limit -= 20; limit; limit--) {
        *buf++ = 'a';
      }
    }
  }
}


  if (auto init_good = tsuba::Init(); !init_good) {
    GALOIS_LOG_FATAL("tsuba::Init: {}", init_good.error());
  }

  init_data(data_100, sizeof(data_100));

#endif
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
