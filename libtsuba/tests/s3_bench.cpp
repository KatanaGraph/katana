#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/time.h>
#include <sys/resource.h>

#include <ctime>
#include <limits>
#include <numeric>
#include <unordered_map>
#include <vector>

#include "galois/Logging.h"
#include "galois/Result.h"
#include "tsuba/tsuba.h"
#include "tsuba/file.h"
#include "bench_utils.h"
#include "tsuba/s3_internal.h"
#include "tsuba/FileAsyncWork.h"

// Benchmarks both tsuba interface and S3 internal interface

constexpr static const char* const s3bucket   = "witchel-tests-east2";
constexpr static const char* const s3obj_base = "s3_test/test-";
constexpr static const char* const kSepStr    = "/";

// TODO: 2020/06/15 - Across different regions

/******************************************************************************/
/* Utilities */

int64_t DivFactor(double us) {
  if (us < 1'000.0) {
    return 1;
  }
  us /= 1'000.0;
  if (us < 1'000.0) {
    return 1'000.0;
  }
  return 1'000'000.0;
}

static const std::unordered_map<int64_t, std::string> df2unit{
    {1, "us"}, {1'000, "ms"}, {1'000'000, " s"}, // '
};

std::string FmtResults(const std::vector<int64_t>& v) {
  if (v.size() == 0) {
    return "no results";
  }
  int64_t sum       = std::accumulate(v.begin(), v.end(), 0L);
  double mean       = (double)sum / v.size();
  int64_t divFactor = DivFactor(mean);

  double accum = 0.0;
  std::for_each(std::begin(v), std::end(v),
                [&](const double d) { accum += (d - mean) * (d - mean); });
  double stdev = 0.0;
  if (v.size() > 1) {
    stdev = sqrt(accum / (v.size() - 1));
  }

  return fmt::format("{:>5.1f} {} (N={:d}) sd {:.1f}", mean / divFactor,
                     df2unit.at(divFactor), v.size(), stdev / divFactor);
}

// 19 chars, with 1 null byte
void get_time_string(char* buf, int32_t limit) {
  time_t rawtime;
  struct tm* timeinfo;
  time(&rawtime);
  timeinfo = localtime(&rawtime);
  strftime(buf, limit, "%Y/%m/%d %H:%M:%S ", timeinfo);
}

void init_data(uint8_t* buf, int32_t limit) {
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

// Thank you, fmt!
std::string CntStr(int32_t i, int32_t width) {
  return fmt::format("{:0{}d}", i, width);
}
std::string MkS3obj(int32_t i, int32_t width) {
  std::string url(s3obj_base);
  return url.append(CntStr(i, width));
}

/******************************************************************************/
// Storage interaction
//    Each function is a timed test, returns vector of times in microseconds
//    (int64_ts)

std::vector<int64_t> test_mem(const uint8_t* data, uint64_t size, int32_t batch,
                              int32_t numExperiments) {
  std::vector<int32_t> fds(batch, 0);
  std::vector<int64_t> results;

  struct timespec start;
  for (auto j = 0; j < numExperiments; ++j) {
    start = now();
    for (auto i = 0; i < batch; ++i) {
      fds[i] = memfd_create(CntStr(i, 4).c_str(), 0);
      if (fds[i] < 0) {
        GALOIS_WARN_ONCE("memfd_create: fd {:04d}: {}", i,
                         galois::ResultErrno().message());
      }
      ssize_t bwritten = write(fds[i], data, size);
      if (bwritten != (ssize_t)size) {
        GALOIS_WARN_ONCE("Short write tried {:d} wrote {:d}: {}", size,
                         bwritten, galois::ResultErrno().message());
      }
    }
    results.push_back(timespec_to_us(timespec_sub(now(), start)));

    for (auto i = 0; i < batch; ++i) {
      int sysret = close(fds[i]);
      if (sysret < 0) {
        GALOIS_WARN_ONCE("close: {}", galois::ResultErrno().message());
      }
    }
  }
  return results;
}

std::vector<int64_t> test_tmp(const uint8_t* data, uint64_t size, int batch,
                              int numExperiments) {
  std::vector<int32_t> fds(batch, 0);
  std::vector<std::string> fnames;
  std::vector<int64_t> results;
  for (auto i = 0; i < batch; ++i) {
    std::string s("/tmp/witchel/");
    fnames.push_back(s.append(CntStr(i, 4)));
  }

  struct timespec start;
  for (auto j = 0; j < numExperiments; ++j) {
    start = now();
    for (auto i = 0; i < batch; ++i) {
      fds[i] = open(fnames[i].c_str(), O_CREAT | O_TRUNC | O_RDWR,
                    S_IRWXU | S_IRWXG);
      if (fds[i] < 0) {
        GALOIS_WARN_ONCE("/tmp O_CREAT: fd {:d}: {}", i,
                         galois::ResultErrno().message());
      }
      ssize_t bwritten = write(fds[i], data, size);
      if (bwritten != (ssize_t)size) {
        GALOIS_WARN_ONCE("Short write tried {:d} wrote {:d}: {}", size,
                         bwritten, galois::ResultErrno().message());
      }
      // Make all data and directory changes persistent
      // sync is overkill, could sync fd and parent directory, but I'm being
      // lazy
      sync();
    }
    for (auto i = 0; i < batch; ++i) {
      int sysret = close(fds[i]);
      if (sysret < 0) {
        GALOIS_LOG_WARN("close: {}", galois::ResultErrno().message());
      }
    }
    results.push_back(timespec_to_us(timespec_sub(now(), start)));

    for (auto i = 0; i < batch; ++i) {
      int sysret = unlink(fnames[i].c_str());
      if (sysret < 0) {
        GALOIS_LOG_WARN("unlink: {}", galois::ResultErrno().message());
      }
    }
  }
  return results;
}

std::vector<int64_t> test_tsuba_sync(const uint8_t* data, uint64_t size,
                                     int32_t batch, int32_t numExperiments) {
  std::vector<std::string> s3urls;
  std::string s3urlstart = "s3://";
  for (auto i = 0; i < batch; ++i) {
    s3urls.push_back(s3urlstart + s3bucket + kSepStr + s3obj_base +
                     MkS3obj(i, 4));
  }
  std::vector<int64_t> results;

  struct timespec start;
  for (auto j = 0; j < numExperiments; ++j) {
    start = now();
    for (const auto& s3url : s3urls) {
      if (auto res = tsuba::FileStore(s3url, data, size); !res) {
        GALOIS_WARN_ONCE("Tsuba store bad return {}\n  {}", res.error(), s3url);
      }
    }
    results.push_back(timespec_to_us(timespec_sub(now(), start)));
  }
  return results;
}

std::vector<int64_t> test_tsuba_async(const uint8_t* data, uint64_t size,
                                      int32_t batch, int32_t numExperiments) {
  std::vector<std::string> s3urls;
  std::vector<std::unique_ptr<tsuba::FileAsyncWork>> async_works{
      (std::size_t)batch};
  std::string s3urlstart = "s3://";
  for (auto i = 0; i < batch; ++i) {
    s3urls.push_back(s3urlstart + s3bucket + kSepStr + s3obj_base +
                     MkS3obj(i, 4));
  }
  std::vector<int64_t> results;

  struct timespec start;
  for (auto j = 0; j < numExperiments; ++j) {
    start = now();
    for (auto i = 0; i < batch; ++i) {
      auto res = tsuba::FileStoreAsync(s3urls[i], data, size);
      if (!res) {
        GALOIS_LOG_ERROR("Tsuba storeasync bad return: {}\n  {}", res.error(),
                         s3urls[i]);
      }
      async_works[i] = std::move(res.value());
    }
    bool done = false;
    while (!done) {
      done = true;
      for (const auto& async_work : async_works) {
        if (async_work != nullptr && !async_work->Done()) {
          done = false;
          if (auto res = (*async_work)(); !res) {
            GALOIS_LOG_ERROR("Tsuba storeasync work bad return {}",
                             res.error());
          }
        }
      }
    }
    results.push_back(timespec_to_us(timespec_sub(now(), start)));
  }
  return results;
}

std::vector<int64_t> test_s3_sync(const uint8_t* data, uint64_t size,
                                  int32_t batch, int32_t numExperiments) {
  std::vector<std::string> s3objs;
  std::vector<int64_t> results;
  for (auto i = 0; i < batch; ++i) {
    s3objs.push_back(MkS3obj(i, 4));
  }

  struct timespec start;
  for (auto j = 0; j < numExperiments; ++j) {
    start = now();
    for (const auto& s3obj : s3objs) {
      // Current API rejects empty writes
      if (auto res =
              tsuba::internal::S3PutSingleSync(s3bucket, s3obj, data, size);
          !res) {
        GALOIS_WARN_ONCE("S3PutSingleSync bad return {}", res.error());
      }
    }
    results.push_back(timespec_to_us(timespec_sub(now(), start)));
  }
  return results;
}

// This one closely tracks s3_sync, not surprisingly
std::vector<int64_t> test_s3_async_one(const uint8_t* data, uint64_t size,
                                       int32_t batch, int32_t numExperiments) {
  std::vector<std::unique_ptr<tsuba::internal::S3AsyncWork>> s3aws;
  std::vector<int64_t> results;
  for (auto i = 0; i < batch; ++i) {
    s3aws.push_back(std::make_unique<tsuba::internal::S3AsyncWork>(
        s3bucket, MkS3obj(i, 4)));
  }

  struct timespec start;
  for (auto j = 0; j < numExperiments; ++j) {
    start = now();
    for (const auto& s3aw : s3aws) {
      // Current API rejects empty writes
      if (auto res = tsuba::internal::S3PutSingleAsync(*s3aw, data, size);
          !res) {
        GALOIS_LOG_ERROR("S3PutSingleAsync return {}", res.error());
      }
      // Only 1 outstanding store at a time
      if (auto res = tsuba::internal::S3PutSingleAsyncFinish(*s3aw); !res) {
        GALOIS_LOG_ERROR("S3PutSingleAsyncFinish bad return {}", res.error());
      }
    }
    results.push_back(timespec_to_us(timespec_sub(now(), start)));
  }
  return results;
}

std::vector<int64_t> test_s3_single_async_batch(const uint8_t* data,
                                                uint64_t size, int32_t batch,
                                                int32_t numExperiments) {
  std::vector<std::unique_ptr<tsuba::internal::S3AsyncWork>> s3aws;
  std::vector<int64_t> results;
  for (auto i = 0; i < batch; ++i) {
    s3aws.push_back(std::make_unique<tsuba::internal::S3AsyncWork>(
        s3bucket, MkS3obj(i, 4)));
  }

  struct timespec start;
  for (auto j = 0; j < numExperiments; ++j) {
    start = now();
    for (const auto& s3aw : s3aws) {
      // Current API rejects empty writes
      if (auto res = tsuba::internal::S3PutSingleAsync(*s3aw, data, size);
          !res) {
        GALOIS_LOG_ERROR("S3PutSingleAsync batch bad return {}", res.error());
      }
    }
    for (const auto& s3aw : s3aws) {
      if (auto res = tsuba::internal::S3PutSingleAsyncFinish(*s3aw); !res) {
        GALOIS_LOG_ERROR("S3PutSingleAsyncFinish batch bad return {}",
                         res.error());
      }
    }
    results.push_back(timespec_to_us(timespec_sub(now(), start)));
  }
  return results;
}

/* These next two benchmarks rely on previous writes. Make sure to call them
 * after at least one write benchmark
 */

std::vector<int64_t> test_s3_async_get_one(const uint8_t* data, uint64_t size,
                                           int32_t batch,
                                           int32_t numExperiments) {
  std::vector<std::unique_ptr<tsuba::internal::S3AsyncWork>> s3aws;
  std::vector<int64_t> results;
  std::vector<uint8_t> read_buffer(size);
  uint8_t* rbuf = read_buffer.data();
  tsuba::StatBuf sbuf;

  for (auto i = 0; i < batch; ++i) {
    s3aws.push_back(std::make_unique<tsuba::internal::S3AsyncWork>(
        s3bucket, MkS3obj(i, 4)));
    // Confirm that the data we need is present
    if (auto res = tsuba::FileStat(
            std::string(s3bucket).append("/").append(MkS3obj(i, 4)), &sbuf);
        !res) {
      GALOIS_LOG_ERROR(
          "tsuba::FileStat({}/{}) returned {}. Did you remember to run the "
          "appropriate write benchmark before this read benchmark?",
          s3bucket, MkS3obj(i, 4), res.error());
    }
    if (sbuf.size != size) {
      GALOIS_LOG_ERROR(
          "{} is of size {}, expected {}. Did you remember to run the "
          "appropriate write benchmark before this read benchmark?",
          std::string(s3bucket).append("/").append(MkS3obj(i, 4)), sbuf.size,
          size);
    }
  }

  struct timespec start;
  for (auto j = 0; j < numExperiments; ++j) {
    memset(rbuf, 0, size);
    start = now();
    for (const auto& s3aw : s3aws) {
      if (auto res = tsuba::internal::S3GetMultiAsync(*s3aw, 0, size, rbuf);
          !res) {
        GALOIS_LOG_ERROR("S3GetMultiAsync return {}", res.error());
      }
      // Only 1 outstanding load at a time
      if (auto res = tsuba::internal::S3GetMultiAsyncFinish(*s3aw); !res) {
        GALOIS_LOG_ERROR("S3GetMultiAsyncFinish bad return {}", res.error());
      }
    }
    results.push_back(timespec_to_us(timespec_sub(now(), start)));
    GALOIS_LOG_ASSERT(!memcmp(rbuf, data, size));
  }
  return results;
}

std::vector<int64_t> test_s3_async_get_batch(const uint8_t* data, uint64_t size,
                                             int32_t batch,
                                             int32_t numExperiments) {
  std::vector<std::unique_ptr<tsuba::internal::S3AsyncWork>> s3aws;
  std::vector<int64_t> results;
  std::vector<uint8_t> read_buffer(size);
  uint8_t* rbuf = read_buffer.data();
  tsuba::StatBuf sbuf;

  for (auto i = 0; i < batch; ++i) {
    s3aws.push_back(std::make_unique<tsuba::internal::S3AsyncWork>(
        s3bucket, MkS3obj(i, 4)));
    // Confirm that the data we need is present
    if (auto res = tsuba::FileStat(
            std::string(s3bucket).append("/").append(MkS3obj(i, 4)), &sbuf);
        !res) {
      GALOIS_LOG_ERROR(
          "tsuba::FileStat({}/{}) returned {}. Did you remember to run the "
          "appropriate write benchmark before this read benchmark?",
          s3bucket, MkS3obj(i, 4), res.error());
    }
    if (sbuf.size != size) {
      GALOIS_LOG_ERROR(
          "{} is of size {}, expected {}. Did you remember to run the "
          "appropriate write benchmark before this read benchmark?",
          std::string(s3bucket).append("/").append(MkS3obj(i, 4)), sbuf.size,
          size);
    }
  }

  struct timespec start;
  for (auto j = 0; j < numExperiments; ++j) {
    memset(rbuf, 0, size);
    start = now();
    for (const auto& s3aw : s3aws) {
      if (auto res = tsuba::internal::S3GetMultiAsync(*s3aw, 0, size, rbuf);
          !res) {
        GALOIS_LOG_ERROR("S3GetMultiAsync batch bad return {}", res.error());
      }
    }
    for (const auto& s3aw : s3aws) {
      if (auto res = tsuba::internal::S3GetMultiAsyncFinish(*s3aw); !res) {
        GALOIS_LOG_ERROR("S3GetMultiAsyncFinish batch bad return {}",
                         res.error());
      }
    }
    results.push_back(timespec_to_us(timespec_sub(now(), start)));
    GALOIS_LOG_ASSERT(!memcmp(rbuf, data, size));
  }
  return results;
}

std::vector<int64_t> test_s3_multi_async_batch(const uint8_t* data,
                                               uint64_t size, int32_t batch,
                                               int32_t numExperiments) {
  std::vector<std::unique_ptr<tsuba::internal::S3AsyncWork>> s3aws;
  std::vector<int64_t> results;
  for (auto i = 0; i < batch; ++i) {
    s3aws.push_back(std::make_unique<tsuba::internal::S3AsyncWork>(
        s3bucket, MkS3obj(i, 4)));
  }

  struct timespec start;
  for (auto j = 0; j < numExperiments; ++j) {
    start = now();
    for (const auto& s3aw : s3aws) {
      // Current API rejects empty writes
      if (auto res = tsuba::internal::S3PutMultiAsync1(*s3aw, data, size);
          !res) {
        GALOIS_LOG_ERROR("S3PutMultiAsync1 bad return {}", res.error());
      }
    }
    for (const auto& s3aw : s3aws) {
      // Current API rejects empty writes
      if (auto res = tsuba::internal::S3PutMultiAsync2(*s3aw); !res) {
        GALOIS_LOG_ERROR("S3PutMultiAsync2 bad return {}", res.error());
      }
    }
    for (const auto& s3aw : s3aws) {
      // Current API rejects empty writes
      if (auto res = tsuba::internal::S3PutMultiAsync3(*s3aw); !res) {
        GALOIS_LOG_ERROR("S3PutMultiAsync3 bad return {}", res.error());
      }
    }
    for (const auto& s3aw : s3aws) {
      if (auto res = tsuba::internal::S3PutMultiAsyncFinish(*s3aw); !res) {
        GALOIS_LOG_ERROR("S3PutMultiAsyncFinish bad return {}", res.error());
      }
    }
    results.push_back(timespec_to_us(timespec_sub(now(), start)));
  }
  return results;
}

/******************************************************************************/
/* Main */

static uint8_t data_19B[19];
static uint8_t data_10MB[10 * (UINT64_C(1) << 20)];
static uint8_t data_100MB[100 * (UINT64_C(1) << 20)];
static uint8_t data_500MB[500 * (UINT64_C(1) << 20)];
static uint8_t data_1GB[(UINT64_C(1) << 30)];

struct {
  uint8_t* data;
  uint64_t size;
  int batch;
  int numExperiments; // For stats
  const char* name;
} datas[] = {
    {.data           = data_19B,
     .size           = sizeof(data_19B),
     .batch          = 8,
     .numExperiments = 3,
     .name           = "  19B"},
    {.data           = data_10MB,
     .size           = sizeof(data_10MB),
     .batch          = 8,
     .numExperiments = 3,
     .name           = " 10MB"},
    {.data           = data_100MB,
     .size           = sizeof(data_100MB),
     .batch          = 8,
     .numExperiments = 3,
     .name           = "100MB"},
    {.data           = data_500MB,
     .size           = sizeof(data_500MB),
     .batch          = 8,
     .numExperiments = 3,
     .name           = "500MB"},
    {.data           = data_1GB,
     .size           = sizeof(data_1GB),
     .batch          = 6,
     .numExperiments = 1,
     .name           = "1GB"},

};

struct {
  const char* name;
  std::vector<int64_t> (*func)(const uint8_t*, uint64_t, int, int);
} tests[] = {
    {.name = "memfd_create", .func = test_mem},
    {.name = "/tmp create", .func = test_tmp},
    // Not needed as it tracks s3_sync
    //    {.name = "S3 Put ASync One", .func = test_s3_async_one},
    // Not needed because it is slow
    //{.name = "S3 Put Sync", .func = test_s3_sync},
    {.name = "S3 Put Single Async Batch", .func = test_s3_single_async_batch},
    // The next two need to follow at least one S3 write benchmark
    {.name = "S3 Get ASync One", .func = test_s3_async_get_one},
    {.name = "S3 Get Async Batch", .func = test_s3_async_get_batch},
    {.name = "Tsuba::FileStore", .func = test_tsuba_sync},
    {.name = "Tsuba::FileStoreAsync", .func = test_tsuba_async},
    {.name = "S3 Put Multi Async Batch", .func = test_s3_multi_async_batch},
};

int main() {
  if (auto init_good = tsuba::Init(); !init_good) {
    GALOIS_LOG_FATAL("tsuba::Init: {}", init_good.error());
  }
  for (uint64_t i = 0; i < sizeof(datas) / sizeof(datas[0]); ++i) {
    init_data(datas[i].data, datas[i].size);
  }

  // TOCTTOU, but I think harmless
  if (access("/tmp/witchel", R_OK) != 0) {
    if (mkdir("/tmp/witchel", 0775) != 0) {
      GALOIS_LOG_WARN("mkdir /tmp/witchel: {}",
                      galois::ResultErrno().message());
      exit(EXIT_FAILURE);
    }
  }

  printf("*** VM and bucket same region\n");
  for (uint64_t i = 0; i < sizeof(datas) / sizeof(datas[0]); ++i) {
    printf("** size %s\n", datas[i].name);
    const uint8_t* data = datas[i].data;
    uint64_t size       = datas[i].size;
    int batch           = datas[i].batch;
    int numExperiments  = datas[i].numExperiments;

    for (uint64_t j = 0; j < sizeof(tests) / sizeof(tests[0]); ++j) {
      std::vector<int64_t> results =
          tests[j].func(data, size, batch, numExperiments);
      fmt::print("{:<25} ({:2d}) {}\n", tests[j].name, batch,
                 FmtResults(results));
    }
  }

  return 0;
}

// 2020/07/14

// *** VM and bucket same region
// ** size   19B
// memfd_create             ( 8)  42.7 us (N=3) sd 21.9
// /tmp create              ( 8)  34.3 ms (N=3) sd 25.9
// S3 Put Async Batch       ( 8)  82.0 ms (N=3) sd 44.5
// Tsuba::FileStore         ( 8) 385.5 ms (N=3) sd 12.1
// Tsuba::FileStoreAsync    ( 8)  22.4 ms (N=3) sd 0.9
// S3 Put Multi Async Batch ( 8) 171.4 ms (N=3) sd 15.0
// ** size  10MB
// memfd_create             ( 8)  35.0 ms (N=3) sd 1.6
// /tmp create              ( 8) 322.0 ms (N=3) sd 66.4
// S3 Put Async Batch       ( 8) 761.0 ms (N=3) sd 4.1
// Tsuba::FileStore         ( 8)   2.4  s (N=3) sd 0.1
// Tsuba::FileStoreAsync    ( 8) 922.8 ms (N=3) sd 87.6
// S3 Put Multi Async Batch ( 8) 886.0 ms (N=3) sd 26.2
// ** size 100MB
// memfd_create             ( 8) 415.8 ms (N=3) sd 10.2
// /tmp create              ( 8)   6.2  s (N=3) sd 0.5
// S3 Put Async Batch       ( 8)   6.9  s (N=3) sd 0.0
// Tsuba::FileStore         ( 8)   8.8  s (N=3) sd 0.1
// Tsuba::FileStoreAsync    ( 8)   7.1  s (N=3) sd 0.1
// S3 Put Multi Async Batch ( 8)   7.0  s (N=3) sd 0.0
// ** size 500MB
// memfd_create             ( 8)   2.0  s (N=3) sd 0.0
// /tmp create              ( 8)  32.3  s (N=3) sd 0.2
// S3 Put Async Batch       ( 8)  34.4  s (N=3) sd 0.1
// Tsuba::FileStore         ( 8)  39.5  s (N=3) sd 5.0
// Tsuba::FileStoreAsync    ( 8)  34.5  s (N=3) sd 0.1
// S3 Put Multi Async Batch ( 8)  34.7  s (N=3) sd 0.1
// ** size 1GB
// memfd_create             ( 6)   3.1  s (N=1) sd 0.0
// /tmp create              ( 6)  50.1  s (N=1) sd 0.0
// S3 Put Async Batch       ( 6)  52.9  s (N=1) sd 0.0
// Tsuba::FileStore         ( 6)  56.5  s (N=1) sd 0.0
// Tsuba::FileStoreAsync    ( 6)  52.9  s (N=1) sd 0.0
// S3 Put Multi Async Batch ( 6)  52.8  s (N=1) sd 0.0

// 2020/06/29

// *** VM and bucket same region
// ** size   19B
// memfd_create             ( 8)  41.7 us (N=3) sd 21.1
// /tmp create              ( 8)  47.3 ms (N=3) sd 44.1
// S3 Put ASync One         ( 8) 261.0 ms (N=3) sd 188.2
// S3 Put Sync              ( 8) 500.0 ms (N=3) sd 172.1
// S3 Put Async Batch       ( 8)  48.7 ms (N=3) sd 30.2
// S3 Put                   ( 8)   1.2  s (N=3) sd 0.0
// S3 Put Multi Async Batch ( 8) 186.6 ms (N=3) sd 34.9
// ** size  10MB
// memfd_create             ( 8)  33.5 ms (N=3) sd 0.7
// /tmp create              ( 8) 345.0 ms (N=3) sd 35.5
// S3 Put ASync One         ( 8)   1.7  s (N=3) sd 0.2
// S3 Put Sync              ( 8)   2.1  s (N=3) sd 0.3
// S3 Put Async Batch       ( 8) 815.8 ms (N=3) sd 73.1
// S3 Put                   ( 8)   2.6  s (N=3) sd 0.3
// S3 Put Multi Async Batch ( 8) 914.9 ms (N=3) sd 30.9
// ** size 100MB
// memfd_create             ( 8) 376.7 ms (N=3) sd 0.2
// /tmp create              ( 8)   6.2  s (N=3) sd 0.5
// S3 Put ASync One         ( 8)  10.3  s (N=3) sd 0.5
// S3 Put Sync              ( 8)  10.7  s (N=3) sd 0.3
// S3 Put Async Batch       ( 8)   6.9  s (N=3) sd 0.0
// S3 Put                   ( 8)   9.1  s (N=3) sd 0.3
// S3 Put Multi Async Batch ( 8)   7.0  s (N=3) sd 0.0
// ** size 500MB
// memfd_create             ( 8)   1.9  s (N=3) sd 0.0
// /tmp create              ( 8)  32.2  s (N=3) sd 0.1
// S3 Put ASync One         ( 8)  50.0  s (N=3) sd 0.8
// S3 Put Sync              ( 8)  47.8  s (N=3) sd 1.6
// S3 Put Async Batch       ( 8)  38.1  s (N=3) sd 6.4
// S3 Put                   ( 8)  36.8  s (N=3) sd 0.8
// S3 Put Multi Async Batch ( 8)  35.6  s (N=3) sd 2.0
// ** size 1GB
// memfd_create             ( 6)   2.9  s (N=1) sd 0.0
// /tmp create              ( 6)  50.1  s (N=1) sd 0.0
// S3 Put ASync One         ( 6)  68.2  s (N=1) sd 0.0
// S3 Put Sync              ( 6)  70.8  s (N=1) sd 0.0
// S3 Put Async Batch       ( 6)  52.6  s (N=1) sd 0.0
// S3 Put                   ( 6)  54.3  s (N=1) sd 0.0
// S3 Put Multi Async Batch ( 6)  52.9  s (N=1) sd 0.0
