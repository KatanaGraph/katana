#include <time.h>

#include "galois/Logging.h"
#include "galois/FileSystem.h"
#include "tsuba/tsuba.h"
#include "tsuba/FileFrame.h"
#include "tsuba/FileView.h"
#include "tsuba/file.h"
#include "tsuba/RDG.h"

// static uint8_t data_100[100];
constexpr static const char* const s3_prop_graphs[] = {
  "s3://witchel-tests-east2/test-0000",
//    "s3://katana-ci/yago-shapes/meta",
    //  "s3://property-graphs/katana/yago-schema/meta",
    //  "s3://property-graphs/katana/ldbc_003/meta",
    //  "s3://property-graphs/katana/yago-shapes/meta",
};

constexpr static const char* const s3_dst_path =
    "s3://witchel-tests-east2/katana-ci/yago-shapes/meta";

void DownloadGraph(const std::string& s3_prop_graph) {
  auto handle_res = tsuba::Open(s3_prop_graph, tsuba::kReadOnly);
  if (!handle_res) {
    GALOIS_LOG_FATAL("Open rdg: {}", handle_res.error());
  }
  auto rdg_res = tsuba::Load(handle_res.value());
  GALOIS_LOG_ASSERT(rdg_res);
  tsuba::RDG rdg = std::move(rdg_res.value());
  // std::shared_ptr<tsuba::RDGHandle> rdg =
  // std::make_shared<tsuba::FileView>(rdg_res.value());

  if (auto rename_res =
          tsuba::Rename(rdg.handle, s3_dst_path, tsuba::kReadWrite);
      !rename_res) {
    GALOIS_LOG_FATAL("Rename to {}: {}", s3_dst_path, rename_res.error());
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

  // auto unique_result = galois::CreateUniqueDirectory(s3_url_base);
  // GALOIS_LOG_ASSERT(unique_result);
  // std::string temp_dir(std::move(unique_result.value()));
  // fmt::print("RRRR {}\n", temp_dir);

  // arrow_file(data_100, sizeof(data_100), temp_dir);

  DownloadGraph(s3_prop_graphs[0]);

  return 0;
}
