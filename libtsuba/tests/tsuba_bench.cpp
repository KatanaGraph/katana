#include "bench_utils.h"
#include "galois/FileSystem.h"
#include "galois/Logging.h"
#include "tsuba/RDG.h"
#include "tsuba/file.h"
#include "tsuba/tsuba.h"

std::string src_uri{};
uint32_t tx_bnc_count{20};
bool opt_transaction_bnc{false};
int opt_verbose_level{0};

std::string prog_name = "tsuba_bench";
std::string usage_msg =
    "Usage: {} <RDG URI>\n"
    "  [-t] execute ARG transactions as fast as possible (default=20)\n"
    "  [-v] verbose, can be repeated (default=false)\n"
    "  [-h] usage message\n";

void
parse_arguments(int argc, char* argv[]) {
  int c;

  while ((c = getopt(argc, argv, "t:vh")) != -1) {
    switch (c) {
    case 't':
      tx_bnc_count = std::atoi(optarg);
      opt_transaction_bnc = true;
      break;
    case 'v':
      opt_verbose_level++;
      break;
    case 'h':
      fmt::print(stderr, usage_msg, prog_name);
      exit(0);
      break;
    default:
      fmt::print(stderr, usage_msg, prog_name);
      exit(EXIT_FAILURE);
    }
  }

  // TODO: Validate paths
  auto index = optind;
  if (index >= argc) {
    fmt::print("{} requires property graph URI argument\n", prog_name);
    exit(EXIT_FAILURE);
  }
  src_uri = argv[index++];
}

void
TxBnc(const std::string& src_uri, int count) {
  struct timespec start = now();

  auto handle_res = tsuba::Open(src_uri, tsuba::kReadWrite);
  if (!handle_res) {
    GALOIS_LOG_FATAL("Open rdg: {}", handle_res.error());
  }
  auto handle = handle_res.value();

  auto rdg_res = tsuba::RDG::Load(handle);
  if (!rdg_res) {
    GALOIS_LOG_FATAL("Load rdg from s3: {}", rdg_res.error());
  }
  auto rdg = std::move(rdg_res.value());
  {
    auto [time, units] = UsToPair(timespec_to_us(timespec_sub(now(), start)));
    fmt::print("Load: {:5.1f}{}\n", time, units);
  }

  start = now();
  for (auto i = 0; i < count; ++i) {
    if (auto res = rdg.Store(handle, "TxBnc"); !res) {
      GALOIS_LOG_FATAL("Store rdg: {}", res.error());
    }
  }
  {
    auto us = timespec_to_us(timespec_sub(now(), start));
    auto [time, units] = UsToPair(us);
    auto [time_tx, units_tx] = UsToPair(count ? (us / count) : us);
    fmt::print("Tx: {:5.1f}{} {:5.1f}{}/tx\n", time, units, time_tx, units_tx);
  }

  if (auto res = tsuba::Close(handle); !res) {
    GALOIS_LOG_FATAL("Close local handle: {}", res.error());
  }
}

////////////////////////////////////////////////////////////////////////

class Experiment {
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
      char tmp[32];              // Generous with space
      get_time_string(tmp, 31);  // Trailing null
      memcpy(buf, tmp, 19);      // Copy without trailing null
      buf += 19;
      if (limit > 19) {
        *buf++ = ' ';
        for (limit -= 20; limit; limit--) {
          *buf++ = 'a';
        }
      }
    }
  }

public:
  std::string name_{};
  uint64_t size_{UINT64_C(0)};
  std::vector<uint8_t> buffer_;
  int batch_{8};
  int numTransfers_{3};  // For stats

  Experiment(const std::string& name, uint64_t size)
      : name_(name), size_(size) {
    buffer_.reserve(size_);
    buffer_.assign(size_, 0);
    init_data(buffer_.data(), size_);
  }
};

std::string
MkS3obj(int32_t i, int32_t width) {
  return fmt::format("test-{:0{}d}", i, width);
}

std::vector<int64_t>
tsuba_put_sync(const Experiment& exp) {
  std::vector<std::string> s3urls;
  for (auto i = 0; i < exp.batch_; ++i) {
    s3urls.push_back(galois::JoinPath(src_uri, MkS3obj(i, 4)));
  }
  std::vector<int64_t> results;

  struct timespec start;
  for (auto j = 0; j < exp.numTransfers_; ++j) {
    start = now();
    for (const auto& s3url : s3urls) {
      if (auto res = tsuba::FileStore(s3url, exp.buffer_.data(), exp.size_);
          !res) {
        GALOIS_WARN_ONCE("Tsuba store bad return {}\n  {}", res.error(), s3url);
      }
    }
    results.push_back(timespec_to_us(timespec_sub(now(), start)));
  }
  return results;
}

std::vector<int64_t>
tsuba_put_async(const Experiment& exp) {
  std::vector<std::string> s3urls;
  std::vector<std::future<galois::Result<void>>> futs{(std::size_t)exp.batch_};
  for (auto i = 0; i < exp.batch_; ++i) {
    s3urls.push_back(galois::JoinPath(src_uri, MkS3obj(i, 4)));
  }
  std::vector<int64_t> results;

  struct timespec start;
  for (auto j = 0; j < exp.numTransfers_; ++j) {
    start = now();
    for (auto i = 0; i < exp.batch_; ++i) {
      auto res =
          tsuba::FileStoreAsync(s3urls[i], exp.buffer_.data(), exp.size_);
      if (!res) {
        GALOIS_LOG_ERROR(
            "Tsuba storeasync bad return: {}\n  {}", res.error(), s3urls[i]);
      }
      futs[i] = std::move(res.value());
    }
    bool done = false;
    while (!done) {
      done = true;
      for (auto& fut : futs) {
        if (fut.valid()) {
          if (auto res = fut.get(); !res) {
            GALOIS_LOG_ERROR(
                "Tsuba storeasync work bad future return {}", res.error());
          }
        }
      }
    }
    results.push_back(timespec_to_us(timespec_sub(now(), start)));
  }
  return results;
}

void
CheckFile(const std::string& url, uint64_t size) {
  // Confirm that the data we need is present
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
std::vector<int64_t>
tsuba_get_async(const Experiment& exp) {
  std::vector<std::string> objects;
  std::vector<int64_t> results;
  std::vector<uint8_t> read_buffer(exp.size_);
  uint8_t* rbuf = read_buffer.data();

  for (auto i = 0; i < exp.batch_; ++i) {
    std::string obj = galois::JoinPath(src_uri, MkS3obj(i, 4));
    objects.push_back(obj);
    // Confirm that the data we need is present
    CheckFile(obj, exp.size_);
  }

  struct timespec start;
  for (auto j = 0; j < exp.numTransfers_; ++j) {
    memset(rbuf, 0, exp.size_);
    std::vector<std::future<galois::Result<void>>> work;
    start = now();
    for (const std::string& object : objects) {
      auto fut_res = tsuba::FilePeekAsync(object, rbuf, 0, exp.size_);
      if (!fut_res) {
        GALOIS_LOG_ERROR("FilePeekAsync: {}", fut_res.error());
      } else {
        work.emplace_back(std::move(fut_res.value()));
      }
    }
    for (auto& fut : work) {
      if (fut.valid()) {
        if (auto res = fut.get(); !res) {
          GALOIS_LOG_ERROR("Work item error: {}", res.error());
        }
      }
    }
    results.push_back(timespec_to_us(timespec_sub(now(), start)));
    GALOIS_LOG_ASSERT(!memcmp(rbuf, exp.buffer_.data(), exp.size_));
  }
  return results;
}

/////////////////////////////////////////////////////////////////////
// Test
struct Test {
  std::string name_;
  std::function<std::vector<int64_t>(const Experiment&)> func_;
  Test(
      const std::string& name,
      std::function<std::vector<int64_t>(const Experiment&)> func)
      : name_(name), func_(func) {}
};
std::vector<Test> tests = {
    Test("Tsuba::FileStore", tsuba_put_sync),
    Test("Tsuba::FileStoreAsync", tsuba_put_async),
    Test("Tsuba::FilePeekAsync", tsuba_get_async),
};

int
main(int argc, char* argv[]) {
  if (auto init_good = tsuba::Init(); !init_good) {
    GALOIS_LOG_FATAL("tsuba::Init: {}", init_good.error());
  }
  parse_arguments(argc, argv);

  if (opt_transaction_bnc) {
    TxBnc(src_uri, tx_bnc_count);
    exit(0);
  }

  std::vector<Experiment> experiments{
      Experiment("  19B", 19), Experiment(" 10MB", 10 * (UINT64_C(1) << 20)),
      Experiment("100MB", 100 * (UINT64_C(1) << 20)),
      Experiment("500MB", 500 * (UINT64_C(1) << 20))
      // Trend for large files is clear at 500MB
      // Experiment("  1GB", UINT64_C(1) << 30)
  };

  for (const auto& exp : experiments) {
    fmt::print("** size {}\n", exp.name_);

    for (const auto& test : tests) {
      std::vector<int64_t> results = test.func_(exp);
      fmt::print(
          "{:<25} ({}) {}\n", test.name_, exp.batch_,
          FmtResults(results, exp.size_));
    }
  }

  return 0;
}
