#include <unistd.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <sys/mman.h>

#include "galois/Logging.h"
#include "galois/Result.h"
#include "galois/FileSystem.h"
#include "tsuba/tsuba.h"
#include "tsuba/file.h"
#include "bench_utils.h"
#include "tsuba/s3_internal.h"
#include "tsuba/FileAsyncWork.h"

// Benchmarks both tsuba interface and S3 internal interface

constexpr static const char* const s3bucket = "witchel-tests-east2";
constexpr static const char* const kSepStr  = "/";

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

std::string FmtResults(const std::vector<int64_t>& v, uint64_t bytes) {
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

  auto[bw, units] = BytesToString(1'000'000 * bytes / mean);
  return fmt::format("{:>5.1f} {} (N={:d}) sd {:5.1f} {:5.1f} {}/s",
                     mean / divFactor, df2unit.at(divFactor), v.size(),
                     stdev / divFactor,
                     bw, units);
}

// 19 chars, with 1 null byte
void get_time_string(char* buf, int32_t limit) {
  time_t rawtime;
  struct tm* timeinfo;
  time(&rawtime);
  timeinfo = localtime(&rawtime);
  strftime(buf, limit, "%Y/%m/%d %H:%M:%S ", timeinfo);
}

void init_data(uint8_t* buf, uint64_t limit) {
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
  constexpr static const char* const s3obj_base = "s3_test/test-";
  std::string url(s3obj_base);
  return url.append(CntStr(i, width));
}
std::string MkS3URL(const std::string& bucket, const std::string& object) {
  constexpr static const char* const s3urlstart = "s3://";
  return s3urlstart + bucket + kSepStr + object;
}

struct Experiment {
  std::string name_{};
  uint64_t size_{UINT64_C(0)};
  std::vector<uint8_t> buffer_;
  int batch_{8};
  int numTransfers_{3}; // For stats

  Experiment(const std::string& name, uint64_t size)
      : name_(name), size_(size) {
    buffer_.reserve(size_);
    buffer_.assign(size_, 0);
    init_data(buffer_.data(), size_);
  }
};

/******************************************************************************/
// Storage interaction
//    Each function is a timed test, returns vector of times in microseconds
//    (int64_ts)

std::vector<int64_t> test_mem(const Experiment& exp) {
  std::vector<int32_t> fds(exp.batch_, 0);
  std::vector<int64_t> results;

  struct timespec start;
  for (auto j = 0; j < exp.numTransfers_; ++j) {
    start = now();
    for (auto i = 0; i < exp.batch_; ++i) {
      fds[i] = memfd_create(CntStr(i, 4).c_str(), 0);
      if (fds[i] < 0) {
        GALOIS_WARN_ONCE("memfd_create: fd {:04d}: {}", i,
                         galois::ResultErrno().message());
      }
      ssize_t bwritten = write(fds[i], exp.buffer_.data(), exp.size_);
      if (bwritten != (ssize_t)exp.size_) {
        GALOIS_WARN_ONCE("Short write tried {:d} wrote {:d}: {}", exp.size_,
                         bwritten, galois::ResultErrno().message());
      }
    }
    results.push_back(timespec_to_us(timespec_sub(now(), start)));

    for (auto i = 0; i < exp.batch_; ++i) {
      int sysret = close(fds[i]);
      if (sysret < 0) {
        GALOIS_WARN_ONCE("close: {}", galois::ResultErrno().message());
      }
    }
  }
  return results;
}

std::vector<int64_t> test_tmp(const Experiment& exp) {
  std::vector<int32_t> fds(exp.batch_, 0);
  std::vector<std::string> fnames;
  std::vector<int64_t> results;
  for (auto i = 0; i < exp.batch_; ++i) {
    std::string s("/tmp/witchel/");
    fnames.push_back(s.append(CntStr(i, 4)));
  }

  struct timespec start;
  for (auto j = 0; j < exp.numTransfers_; ++j) {
    start = now();
    for (auto i = 0; i < exp.batch_; ++i) {
      fds[i] = open(fnames[i].c_str(), O_CREAT | O_TRUNC | O_RDWR,
                    S_IRWXU | S_IRWXG);
      if (fds[i] < 0) {
        GALOIS_WARN_ONCE("/tmp O_CREAT: fd {:d}: {}", i,
                         galois::ResultErrno().message());
      }
      ssize_t bwritten = write(fds[i], exp.buffer_.data(), exp.size_);
      if (bwritten != (ssize_t)exp.size_) {
        GALOIS_WARN_ONCE("Short write tried {:d} wrote {:d}: {}", exp.size_,
                         bwritten, galois::ResultErrno().message());
      }
      // Make all data and directory changes persistent
      // sync is overkill, could sync fd and parent directory, but I'm being
      // lazy
      sync();
    }
    for (auto i = 0; i < exp.batch_; ++i) {
      int sysret = close(fds[i]);
      if (sysret < 0) {
        GALOIS_LOG_WARN("close: {}", galois::ResultErrno().message());
      }
    }
    results.push_back(timespec_to_us(timespec_sub(now(), start)));

    for (auto i = 0; i < exp.batch_; ++i) {
      int sysret = unlink(fnames[i].c_str());
      if (sysret < 0) {
        GALOIS_LOG_WARN("unlink: {}", galois::ResultErrno().message());
      }
    }
  }
  return results;
}

std::vector<int64_t> test_tsuba_sync(const Experiment& exp) {
  std::vector<std::string> s3urls;
  for (auto i = 0; i < exp.batch_; ++i) {
    s3urls.push_back(MkS3URL(s3bucket, MkS3obj(i, 4)));
  }
  std::vector<int64_t> results;

  struct timespec start;
  for (auto j = 0; j < exp.numTransfers_; ++j) {
    start = now();
    for (const auto& s3url : s3urls) {
      if (auto res = tsuba::FileStore(s3url, exp.buffer_.data(), exp.size_); !res) {
        GALOIS_WARN_ONCE("Tsuba store bad return {}\n  {}", res.error(), s3url);
      }
    }
    results.push_back(timespec_to_us(timespec_sub(now(), start)));
  }
  return results;
}

std::vector<int64_t> test_tsuba_async(const Experiment& exp) {
  std::vector<std::string> s3urls;
  std::vector<std::unique_ptr<tsuba::FileAsyncWork>> async_works{
      (std::size_t)exp.batch_};
  for (auto i = 0; i < exp.batch_; ++i) {
    s3urls.push_back(MkS3URL(s3bucket, MkS3obj(i, 4)));
  }
  std::vector<int64_t> results;

  struct timespec start;
  for (auto j = 0; j < exp.numTransfers_; ++j) {
    start = now();
    for (auto i = 0; i < exp.batch_; ++i) {
      auto res = tsuba::FileStoreAsync(s3urls[i], exp.buffer_.data(), exp.size_);
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

std::vector<int64_t> test_s3_sync(const Experiment& exp) {
  std::vector<std::string> s3objs;
  std::vector<int64_t> results;
  for (auto i = 0; i < exp.batch_; ++i) {
    s3objs.push_back(MkS3obj(i, 4));
  }

  struct timespec start;
  for (auto j = 0; j < exp.numTransfers_; ++j) {
    start = now();
    for (const auto& s3obj : s3objs) {
      // Current API rejects empty writes
      if (auto res = tsuba::internal::S3PutSingleSync(s3bucket, s3obj,
                                                      exp.buffer_.data(), exp.size_);
          !res) {
        GALOIS_WARN_ONCE("S3PutSingleSync bad return {}", res.error());
      }
    }
    results.push_back(timespec_to_us(timespec_sub(now(), start)));
  }
  return results;
}

// This one closely tracks s3_sync, not surprisingly
std::vector<int64_t> test_s3_async_one(const Experiment& exp) {
  std::vector<std::unique_ptr<tsuba::internal::S3AsyncWork>> s3aws;
  std::vector<int64_t> results;
  for (auto i = 0; i < exp.batch_; ++i) {
    s3aws.push_back(std::make_unique<tsuba::internal::S3AsyncWork>(
        s3bucket, MkS3obj(i, 4)));
  }

  struct timespec start;
  for (auto j = 0; j < exp.numTransfers_; ++j) {
    start = now();
    for (const auto& s3aw : s3aws) {
      // Current API rejects empty writes
      if (auto res =
          tsuba::internal::S3PutSingleAsync(*s3aw, exp.buffer_.data(), exp.size_);
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

std::vector<int64_t> test_s3_single_async_batch(const Experiment& exp) {
  std::vector<std::unique_ptr<tsuba::internal::S3AsyncWork>> s3aws;
  std::vector<int64_t> results;
  for (auto i = 0; i < exp.batch_; ++i) {
    s3aws.push_back(std::make_unique<tsuba::internal::S3AsyncWork>(
        s3bucket, MkS3obj(i, 4)));
  }

  struct timespec start;
  for (auto j = 0; j < exp.numTransfers_; ++j) {
    start = now();
    for (const auto& s3aw : s3aws) {
      if (auto res =
          tsuba::internal::S3PutSingleAsync(*s3aw, exp.buffer_.data(), exp.size_);
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

void CheckFile(const std::string& bucket, const std::string& object,
               uint64_t size) {
  // Confirm that the data we need is present
  std::string url = MkS3URL(bucket, object);
  tsuba::StatBuf sbuf;

  if (auto res = tsuba::FileStat(url, &sbuf); !res) {
    GALOIS_LOG_ERROR(
        "tsuba::FileStat({}) returned {}. Did you remember to run the "
        "appropriate write benchmark before this read benchmark?",
        url, sbuf.size);
  }
  if (sbuf.size != size) {
    GALOIS_LOG_ERROR(
        "{} is of size {}, expected {}. Did you remember to run the "
        "appropriate write benchmark before this read benchmark?",
        url, sbuf.size, size);
  }
}

/* These next two benchmarks rely on previous writes. Make sure to call them
 * after at least one write benchmark
 */
std::vector<int64_t> test_s3_async_get_one(const Experiment& exp) {
  std::vector<std::unique_ptr<tsuba::internal::S3AsyncWork>> s3aws;
  std::vector<int64_t> results;
  std::vector<uint8_t> read_buffer(exp.size_);
  uint8_t* rbuf = read_buffer.data();

  for (auto i = 0; i < exp.batch_; ++i) {
    s3aws.push_back(std::make_unique<tsuba::internal::S3AsyncWork>(
        s3bucket, MkS3obj(i, 4)));
    // Confirm that the data we need is present
    CheckFile(s3bucket, MkS3obj(i, 4), exp.size_);
  }

  struct timespec start;
  for (auto j = 0; j < exp.numTransfers_; ++j) {
    memset(rbuf, 0, exp.size_);
    start = now();
    for (const auto& s3aw : s3aws) {
      if (auto res =
              tsuba::internal::S3GetMultiAsync(*s3aw, 0, exp.size_, rbuf);
          !res) {
        GALOIS_LOG_ERROR("S3GetMultiAsync return {}", res.error());
      }
      // Only 1 outstanding load at a time
      if (auto res = tsuba::internal::S3GetMultiAsyncFinish(*s3aw); !res) {
        GALOIS_LOG_ERROR("S3GetMultiAsyncFinish bad return {}", res.error());
      }
    }
    results.push_back(timespec_to_us(timespec_sub(now(), start)));
    GALOIS_LOG_ASSERT(!memcmp(rbuf, exp.buffer_.data(), exp.size_));
  }
  return results;
}

std::vector<int64_t> test_s3_async_get_batch(const Experiment& exp) {
  std::vector<std::unique_ptr<tsuba::internal::S3AsyncWork>> s3aws;
  std::vector<int64_t> results;
  std::vector<uint8_t> read_buffer(exp.size_);
  uint8_t* rbuf = read_buffer.data();
  tsuba::StatBuf sbuf;

  for (auto i = 0; i < exp.batch_; ++i) {
    s3aws.push_back(std::make_unique<tsuba::internal::S3AsyncWork>(
        s3bucket, MkS3obj(i, 4)));
    // Confirm that the data we need is present
    CheckFile(s3bucket, MkS3obj(i, 4), exp.size_);
  }

  struct timespec start;
  for (auto j = 0; j < exp.numTransfers_; ++j) {
    memset(rbuf, 0, exp.size_);
    start = now();
    for (const auto& s3aw : s3aws) {
      if (auto res =
              tsuba::internal::S3GetMultiAsync(*s3aw, 0, exp.size_, rbuf);
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
    GALOIS_LOG_ASSERT(!memcmp(rbuf, exp.buffer_.data(), exp.size_));
  }
  return results;
}

std::vector<int64_t> test_s3_multi_async_batch(const Experiment& exp) {
  std::vector<std::unique_ptr<tsuba::internal::S3AsyncWork>> s3aws;
  std::vector<int64_t> results;
  for (auto i = 0; i < exp.batch_; ++i) {
    s3aws.push_back(std::make_unique<tsuba::internal::S3AsyncWork>(
        s3bucket, MkS3obj(i, 4)));
  }

  struct timespec start;
  for (auto j = 0; j < exp.numTransfers_; ++j) {
    start = now();
    for (const auto& s3aw : s3aws) {
      // Current API rejects empty writes
      if (auto res =
          tsuba::internal::S3PutMultiAsync1(*s3aw, exp.buffer_.data(), exp.size_);
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

struct Test {
  std::string name_;
  std::function<std::vector<int64_t>(const Experiment&)> func_;
  Test(const std::string& name, std::function<std::vector<int64_t>(const Experiment&)> func)
    : name_(name), func_(func) {
  }
};
std::vector<Test> tests = {
  Test("memfd_create", test_mem),
  Test("/tmp create", test_tmp),
  // Not needed as it tracks s3_sync
  //    Test("S3 Put ASync One", test_s3_async_one),
  // Not needed because it is slow
  Test("S3 Put Sync", test_s3_sync),
  Test("S3 Put Single Async Batch", test_s3_single_async_batch),
    // The next two need to follow at least one S3 write benchmark
  Test("S3 Get ASync One", test_s3_async_get_one),
  Test("S3 Get Async Batch", test_s3_async_get_batch),
  Test("Tsuba::FileStore", test_tsuba_sync),
  Test("Tsuba::FileStoreAsync", test_tsuba_async),
  Test("S3 Put Multi Async Batch", test_s3_multi_async_batch),
};

int main() {
  if (auto init_good = tsuba::Init(); !init_good) {
    GALOIS_LOG_FATAL("tsuba::Init: {}", init_good.error());
  }
  std::vector<Experiment> experiments{
      Experiment("  19B", 19), Experiment(" 10MB", 10 * (UINT64_C(1) << 20)),
      Experiment("100MB", 100 * (UINT64_C(1) << 20)),
      Experiment("500MB", 500 * (UINT64_C(1) << 20))
      // Trend for large files is clear at 500MB
      // Experiment("  1GB", UINT64_C(1) << 30)
  };

  // TOCTTOU, but I think harmless
  if (access("/tmp/witchel", R_OK) != 0) {
    if (mkdir("/tmp/witchel", 0775) != 0) {
      GALOIS_LOG_WARN("mkdir /tmp/witchel: {}",
                      galois::ResultErrno().message());
      exit(EXIT_FAILURE);
    }
  }

  fmt::print("*** VM and bucket same region\n");
  for (const auto& exp : experiments) {
    fmt::print("** size {}\n", exp.name_);

    for(const auto& test: tests) {
      std::vector<int64_t> results = test.func_(exp);
      fmt::print("{:<25} ({}) {}\n", test.name_, exp.batch_,
                 FmtResults(results, exp.size_));
    }
  }

  return 0;
}

// 2020/08/29

//*** VM and bucket same region
//** size   19B
// memfd_create              (8)  59.0 us (N=3) sd  38.6 314.5 KB/s
// /tmp create               (8)  42.5 ms (N=3) sd  34.2 447.0 B/s
// S3 Put Single Async Batch (8)  36.1 ms (N=3) sd  26.5 526.0 B/s
// S3 Get ASync One          (8)  73.7 ms (N=3) sd   4.4 257.0 B/s
// S3 Get Async Batch        (8)  17.9 ms (N=3) sd  10.1   1.0 KB/s
// Tsuba::FileStore          (8) 337.4 ms (N=3) sd  15.9  56.0 B/s
// Tsuba::FileStoreAsync     (8)  20.9 ms (N=3) sd   1.3 908.0 B/s
// S3 Put Multi Async Batch  (8) 172.0 ms (N=3) sd  18.8 110.0 B/s
// ** size  10MB
// memfd_create              (8)  42.8 ms (N=3) sd   2.1 233.9 MB/s
// /tmp create               (8) 320.1 ms (N=3) sd 110.4  31.2 MB/s
// S3 Put Single Async Batch (8) 334.5 ms (N=3) sd  26.3  29.9 MB/s
// S3 Get ASync One          (8) 711.7 ms (N=3) sd  42.2  14.1 MB/s
// S3 Get Async Batch        (8) 368.3 ms (N=3) sd  13.8  27.1 MB/s
// Tsuba::FileStore          (8)   2.7  s (N=3) sd   0.1   3.7 MB/s
// Tsuba::FileStoreAsync     (8) 589.6 ms (N=3) sd 103.3  17.0 MB/s
// S3 Put Multi Async Batch  (8) 465.2 ms (N=3) sd  41.2  21.5 MB/s
// ** size 100MB
// memfd_create              (8) 532.0 ms (N=3) sd 115.7 188.0 MB/s
// /tmp create               (8)   6.2  s (N=3) sd   0.4  16.2 MB/s
// S3 Put Single Async Batch (8)   2.4  s (N=3) sd   0.2  41.1 MB/s
// S3 Get ASync One          (8)   3.5  s (N=3) sd   0.1  28.9 MB/s
// S3 Get Async Batch        (8)   3.4  s (N=3) sd   0.0  29.4 MB/s
// Tsuba::FileStore          (8)   7.0  s (N=3) sd   2.2  14.3 MB/s
// Tsuba::FileStoreAsync     (8)   3.0  s (N=3) sd   0.5  33.8 MB/s
// S3 Put Multi Async Batch  (8)   3.6  s (N=3) sd   1.4  27.5 MB/s
// ** size 500MB
// memfd_create              (8)   2.2  s (N=3) sd   0.0 226.5 MB/s
// /tmp create               (8)  32.2  s (N=3) sd   0.0  15.5 MB/s
// S3 Put Single Async Batch (8)  34.4  s (N=3) sd   0.0  14.5 MB/s
// S3 Get ASync One          (8)  34.4  s (N=3) sd   0.1  14.5 MB/s
// S3 Get Async Batch        (8)  34.3  s (N=3) sd   0.1  14.6 MB/s
// Tsuba::FileStore          (8)  36.6  s (N=3) sd   0.5  13.7 MB/s
// Tsuba::FileStoreAsync     (8)  34.5  s (N=3) sd   0.0  14.5 MB/s
// S3 Put Multi Async Batch  (8)  34.6  s (N=3) sd   0.0  14.5 MB/s
// ** size 1GB
// memfd_create              ( 6)   3.1  s (N=1) sd 0.0
// /tmp create               ( 6)  50.1  s (N=1) sd 0.0
// S3 Put Single Async Batch ( 6)  53.1  s (N=1) sd 0.0
// S3 Get ASync One          ( 6)  52.7  s (N=1) sd 0.0
// S3 Get Async Batch        ( 6)  52.6  s (N=1) sd 0.0
// Tsuba::FileStore          ( 6)  54.1  s (N=1) sd 0.0
// Tsuba::FileStoreAsync     ( 6)  52.9  s (N=1) sd 0.0
// S3 Put Multi Async Batch  ( 6)  53.0  s (N=1) sd 0.0


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
