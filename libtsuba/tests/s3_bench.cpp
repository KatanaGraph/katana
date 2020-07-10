#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/time.h>
#include <sys/resource.h>
#include <time.h>

#include <vector>
#include <limits>
#include <numeric>

#include "galois/Logging.h"
#include "tsuba/tsuba.h"
#include "tsuba/file.h"

constexpr static const char* const s3_url_base =
    "s3://witchel-tests-east2/test-";

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
    {1, "us"},
    {1'000, "ms"},
    {1'000'000, " s"},
};

std::string FmtResults(const std::vector<int64_t>& v) {
  if (v.size() == 0)
    return "no results";
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

struct timespec now() {
  struct timespec tp;
  // CLOCK_BOOTTIME is probably better, but Linux specific
  int ret = clock_gettime(CLOCK_MONOTONIC, &tp);
  if (ret < 0) {
    perror("clock_gettime");
    GALOIS_LOG_ERROR("Bad return");
  }
  return tp;
}

struct timespec timespec_sub(struct timespec time, struct timespec oldTime) {
  if (time.tv_nsec < oldTime.tv_nsec)
    return (struct timespec){.tv_sec  = time.tv_sec - 1 - oldTime.tv_sec,
                             .tv_nsec = 1'000'000'000L + time.tv_nsec -
                                        oldTime.tv_nsec};
  else
    return (struct timespec){.tv_sec  = time.tv_sec - oldTime.tv_sec,
                             .tv_nsec = time.tv_nsec - oldTime.tv_nsec};
}

int64_t timespec_to_us(struct timespec ts) {
  return ts.tv_sec * 1'000'000 + ts.tv_nsec / 1'000;
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
std::string MkS3url(int32_t i, int32_t width) {
  std::string url(s3_url_base);
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
        GALOIS_LOG_ERROR("fd {:04d}", i);
        perror("memfd_create");
      }
      ssize_t bwritten = write(fds[i], data, size);
      if (bwritten != (ssize_t)size) {
        GALOIS_LOG_ERROR("Short write tried {:d} wrote {:d}", size, bwritten);
        perror("write");
      }
    }
    results.push_back(timespec_to_us(timespec_sub(now(), start)));

    for (auto i = 0; i < batch; ++i) {
      int sysret = close(fds[i]);
      if (sysret < 0) {
        perror("close");
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
        GALOIS_LOG_ERROR("fd {:d}", i);
        perror("/tmp O_CREAT");
      }
      ssize_t bwritten = write(fds[i], data, size);
      if (bwritten != (ssize_t)size) {
        GALOIS_LOG_ERROR("Short write tried {:d} wrote {:d}", size, bwritten);
        perror("write");
      }
      // Make all data and directory changes persistent
      // sync is overkill, could sync fd and parent directory, but I'm being
      // lazy
      sync();
    }
    for (auto i = 0; i < batch; ++i) {
      int sysret = close(fds[i]);
      if (sysret < 0) {
        perror("close");
      }
    }
    results.push_back(timespec_to_us(timespec_sub(now(), start)));

    for (auto i = 0; i < batch; ++i) {
      int sysret = unlink(fnames[i].c_str());
      if (sysret < 0) {
        perror("unlink");
      }
    }
  }
  return results;
}

std::vector<int64_t> test_s3(const uint8_t* data, uint64_t size, int32_t batch,
                             int32_t numExperiments) {
  std::vector<int64_t> results;

  struct timespec start;
  for (auto j = 0; j < numExperiments; ++j) {
    start = now();
    for (auto i = 0; i < batch; ++i) {
      std::string s3url = MkS3url(i, 4);
      if (auto res = tsuba::FileStore(s3url, data, size); !res) {
        GALOIS_LOG_ERROR("Tsuba store bad return {}", res.error());
      }
    }
    results.push_back(timespec_to_us(timespec_sub(now(), start)));
  }
  return results;
}

std::vector<int64_t> test_s3_sync(const uint8_t* data, uint64_t size,
                                  int32_t batch, int32_t numExperiments) {
  std::vector<std::string> s3urls;
  std::vector<int64_t> results;
  for (auto i = 0; i < batch; ++i) {
    s3urls.push_back(MkS3url(i, 4));
  }

  struct timespec start;
  for (auto j = 0; j < numExperiments; ++j) {
    start = now();
    for (const auto& s3url : s3urls) {
      // Current API rejects empty writes
      if (auto res = tsuba::FileStoreSync(s3url, data, size); !res) {
        GALOIS_LOG_ERROR("Tsuba store sync bad return {}", res.error());
      }
    }
    results.push_back(timespec_to_us(timespec_sub(now(), start)));
  }
  return results;
}

std::vector<int64_t> test_s3_async_one(const uint8_t* data, uint64_t size,
                                       int32_t batch, int32_t numExperiments) {
  std::vector<std::string> s3urls;
  std::vector<int64_t> results;
  for (auto i = 0; i < batch; ++i) {
    s3urls.push_back(MkS3url(i, 4));
  }

  struct timespec start;
  for (auto j = 0; j < numExperiments; ++j) {
    start = now();
    for (const auto& s3url : s3urls) {
      // Current API rejects empty writes
      if (auto res = tsuba::FileStoreAsync(s3url, data, size); !res) {
        GALOIS_LOG_ERROR("Tsuba store async bad return {}", res.error());
      }
      // Only 1 outstanding store at a time
      if (auto res = tsuba::FileStoreAsyncFinish(s3url); !res) {
        GALOIS_LOG_ERROR("Tsuba store async finish bad return {}", res.error());
      }
    }
    results.push_back(timespec_to_us(timespec_sub(now(), start)));
  }
  return results;
}

std::vector<int64_t> test_s3_async_batch(const uint8_t* data, uint64_t size,
                                         int32_t batch,
                                         int32_t numExperiments) {
  std::vector<std::string> s3urls;
  std::vector<int64_t> results;
  for (auto i = 0; i < batch; ++i) {
    s3urls.push_back(MkS3url(i, 4));
  }

  struct timespec start;
  for (auto j = 0; j < numExperiments; ++j) {
    start = now();
    for (const auto& s3url : s3urls) {
      // Current API rejects empty writes
      if (auto res = tsuba::FileStoreAsync(s3url, data, size); !res) {
        GALOIS_LOG_ERROR("Tsuba store async bad return {}", res.error());
      }
    }
    for (const auto& s3url : s3urls) {
      if (auto res = tsuba::FileStoreAsyncFinish(s3url); !res) {
        GALOIS_LOG_ERROR("Tsuba store async bad return {}", res.error());
      }
    }
    results.push_back(timespec_to_us(timespec_sub(now(), start)));
  }
  return results;
}

std::vector<int64_t> test_s3_multi_async_batch(const uint8_t* data,
                                               uint64_t size, int32_t batch,
                                               int32_t numExperiments) {
  std::vector<std::string> s3urls;
  std::vector<int64_t> results;
  for (auto i = 0; i < batch; ++i) {
    s3urls.push_back(MkS3url(i, 4));
  }

  struct timespec start;
  for (auto j = 0; j < numExperiments; ++j) {
    start = now();
    for (const auto& s3url : s3urls) {
      // Current API rejects empty writes
      if (auto res = tsuba::FileStoreMultiAsync1(s3url, data, size); !res) {
        GALOIS_LOG_ERROR("Tsuba store multi async1 bad return {}", res.error());
      }
    }
    for (const auto& s3url : s3urls) {
      // Current API rejects empty writes
      if (auto res = tsuba::FileStoreMultiAsync2(s3url); !res) {
        GALOIS_LOG_ERROR("Tsuba store multi async2 bad return {}", res.error());
      }
    }
    for (const auto& s3url : s3urls) {
      // Current API rejects empty writes
      if (auto res = tsuba::FileStoreMultiAsync3(s3url); !res) {
        GALOIS_LOG_ERROR("Tsuba store multi async3 bad return {}", res.error());
      }
    }
    for (const auto& s3url : s3urls) {
      if (auto res = tsuba::FileStoreMultiAsyncFinish(s3url); !res) {
        GALOIS_LOG_ERROR("Tsuba store multi async finish bad return {}",
                         res.error());
      }
    }
    results.push_back(timespec_to_us(timespec_sub(now(), start)));
  }
  return results;
}

/******************************************************************************/
/* Main */

static uint8_t data_19B[19];
static uint8_t data_10MB[10 * (1UL << 20)];
static uint8_t data_100MB[100 * (1UL << 20)];
static uint8_t data_500MB[500 * (1UL << 20)];
static uint8_t data_1GB[(1UL << 30)];

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
    {.name = "S3 Put ASync One", .func = test_s3_async_one},
    {.name = "S3 Put Sync", .func = test_s3_sync},
    {.name = "S3 Put Async Batch", .func = test_s3_async_batch},
    {.name = "S3 Put", .func = test_s3},
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
      perror("mkdir /tmp/witchel");
      exit(10);
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
      fmt::print("{:<24} ({:2d}) {}\n", tests[j].name, batch,
                 FmtResults(results));
    }
  }

  return 0;
}

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
