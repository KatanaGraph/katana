#include <boost/filesystem.hpp>

#include "galois/Logging.h"
#include "galois/FileSystem.h"
#include "tsuba/FileFrame.h"
#include "tsuba/FileView.h"
#include "tsuba/file.h"
#include "tsuba/tsuba.h"

namespace fs = boost::filesystem;

#define EXP_WRITE_COUNT 15
#define READ_PARTIAL 4567

static void fill_bits(uint8_t bits[], int n) {
  for (int i = 0; i < n; ++i) {
    bits[i] = std::rand();
  }
}

static void exponential(uint8_t bits[], std::string& dir) {
  // Write
  std::string filename = dir + "exponential";
  auto ff              = tsuba::FileFrame();
  if (auto res = ff.Init(); !res) {
    GALOIS_LOG_FATAL("Init: {}", res.error());
  }

  uint8_t* ptr     = bits;
  uint64_t running = 0;
  for (int i = 0; i < EXP_WRITE_COUNT; ++i) {
    arrow::Status aro_sts = ff.Write(ptr, 1 << i);
    GALOIS_LOG_ASSERT(aro_sts.ok());
    ptr += 1 << i;
    running += 1 << i;
  }
  ff.Bind(filename);
  if (auto res = ff.Persist(); !res) {
    GALOIS_LOG_FATAL("Persist: {}", res.error());
  }

  // Validate
  tsuba::StatBuf buf;
  if (auto res = tsuba::FileStat(filename, &buf); !res) {
    GALOIS_LOG_FATAL("FileStat: {}", res.error());
  }
  GALOIS_LOG_ASSERT(buf.size == running);

  // Read
  auto fv = tsuba::FileView();
  if (auto res = fv.Bind(filename, true); !res) {
    GALOIS_LOG_FATAL("Bind on {}: {}", filename, res.error());
  }
  auto aro_res = fv.Read(running);
  GALOIS_LOG_ASSERT(aro_res.ok());
  auto aro_buf = aro_res.ValueOrDie();
  GALOIS_LOG_ASSERT(static_cast<uint64_t>(aro_buf->size()) == running);
  GALOIS_LOG_ASSERT(!memcmp(aro_buf->data(), bits, running));

  // Exercise asynchronous reads
  auto fva = tsuba::FileView();
  if (auto res = fva.Bind(filename, 0, 0, false); !res) {
    GALOIS_LOG_FATAL("Bind on {}: {}", filename, res.error());
  }

  ptr     = bits;
  running = 0;
  for (int i = 0; i < EXP_WRITE_COUNT; ++i) {
    aro_res = fva.Read(1 << i);
    GALOIS_LOG_ASSERT(aro_res.ok());
    aro_buf = aro_res.ValueOrDie();
    GALOIS_LOG_ASSERT(!memcmp(ptr, aro_buf->data(), 1 << i));
    ptr += 1 << i;
    running += 1 << i;
  }
}

static void the_big_one(uint8_t bits[], uint64_t num_bytes, std::string& dir) {
  // Write
  std::string filename = dir + "the-big-one";
  auto ff              = tsuba::FileFrame();
  if (auto res = ff.Init(); !res) {
    GALOIS_LOG_FATAL("Init: {}", res.error());
  }

  arrow::Status aro_sts = ff.Write(bits, num_bytes);
  GALOIS_LOG_ASSERT(aro_sts.ok());
  ff.Bind(filename);
  if (auto res = ff.Persist(); !res) {
    GALOIS_LOG_FATAL("Persist: {}", res.error());
  }

  // Validate
  tsuba::StatBuf buf;
  if (auto res = tsuba::FileStat(filename, &buf); !res) {
    GALOIS_LOG_FATAL("FileStat: {}", res.error());
  }
  GALOIS_LOG_ASSERT(buf.size == num_bytes);

  // Read
  uint64_t res[num_bytes];
  auto fv = tsuba::FileView();
  if (auto res = fv.Bind(filename, true); !res) {
    GALOIS_LOG_FATAL("Bind on {}: {}", filename, res.error());
  }
  arrow::Result<int64_t> aro_res = fv.Read(READ_PARTIAL, res);
  GALOIS_LOG_ASSERT(aro_res.ok());
  int64_t bytes_read = aro_res.ValueOrDie();
  GALOIS_LOG_ASSERT(bytes_read == READ_PARTIAL);
  GALOIS_LOG_ASSERT(!memcmp(res, bits, READ_PARTIAL));
}

static void silly(uint8_t bits[], uint64_t num_bytes, std::string& dir) {
  // Write
  std::string filename = dir + "silly";
  auto ff              = tsuba::FileFrame();
  if (auto res = ff.Init(num_bytes * 2); !res) {
    GALOIS_LOG_FATAL("Init: {}", res.error());
  }

  if (auto res = ff.Persist(); res) {
    GALOIS_LOG_FATAL("Persist should have failed");
  }

  auto aro_buf          = std::make_shared<arrow::Buffer>(bits, num_bytes);
  arrow::Status aro_sts = ff.Write(aro_buf);
  GALOIS_LOG_ASSERT(aro_sts.ok());
  if (auto res = ff.Persist(); res) {
    GALOIS_LOG_FATAL("Persist should have failed");
  }
  ff.Bind(filename);
  if (auto res = ff.Persist(); !res) {
    GALOIS_LOG_FATAL("Persist: {}", res.error());
  }

  // Validate
  tsuba::StatBuf buf;
  if (auto res = tsuba::FileStat(filename, &buf); !res) {
    GALOIS_LOG_FATAL("FileStat: {}", res.error());
  }
  GALOIS_LOG_ASSERT(buf.size == num_bytes);

  // Read
  auto fv = tsuba::FileView();
  if (auto res = fv.Bind(filename + "not-a-file", true); res) {
    GALOIS_LOG_FATAL("Bind should have failed!");
  }

  if (auto res = fv.Bind(filename, true); !res) {
    GALOIS_LOG_FATAL("Bind on {}: {}", filename, res.error());
  }

  aro_sts = fv.Seek(num_bytes - READ_PARTIAL);
  GALOIS_LOG_ASSERT(aro_sts.ok());
  arrow::Result<int64_t> aro_res = fv.Tell();
  GALOIS_LOG_ASSERT(aro_res.ok());
  GALOIS_LOG_ASSERT(static_cast<uint64_t>(aro_res.ValueOrDie()) ==
                    num_bytes - READ_PARTIAL);

  arrow::Result<std::shared_ptr<arrow::Buffer>> aro_rest = fv.Read(num_bytes);
  GALOIS_LOG_ASSERT(aro_rest.ok());
  auto aro_buff = aro_rest.ValueOrDie();
  GALOIS_LOG_ASSERT(static_cast<uint64_t>(aro_buff->size()) == READ_PARTIAL);
  GALOIS_LOG_ASSERT(
      !memcmp(aro_buff->data(), &bits[num_bytes - READ_PARTIAL], READ_PARTIAL));

  aro_sts = fv.Close();
  GALOIS_LOG_ASSERT(aro_sts.ok());
  GALOIS_LOG_ASSERT(fv.closed());
  aro_sts = ff.Close();
  GALOIS_LOG_ASSERT(aro_sts.ok());
  GALOIS_LOG_ASSERT(ff.closed());
}

int main() {
  if (auto res = tsuba::Init(); !res) {
    GALOIS_LOG_FATAL("tsuba::Init: {}", res.error());
  }
  uint64_t num_bytes = 1 << EXP_WRITE_COUNT;
  uint8_t bits[num_bytes];
  fill_bits(bits, num_bytes);

  auto unique_result = galois::CreateUniqueDirectory("/tmp/fileobjects-");
  GALOIS_LOG_ASSERT(unique_result);
  std::string temp_dir(std::move(unique_result.value()));

  exponential(bits, temp_dir);
  the_big_one(bits, num_bytes, temp_dir);
  silly(bits, num_bytes, temp_dir);

  fs::remove_all(temp_dir);
  if (auto res = tsuba::Fini(); !res) {
    GALOIS_LOG_FATAL("tsuba::Fini: {}", res.error());
  }
  return 0;
}
