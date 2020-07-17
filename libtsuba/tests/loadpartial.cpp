#include <random>

#include "arrow/api.h"
#include <boost/filesystem.hpp>

#include "galois/Logging.h"
#include "galois/FileSystem.h"
#include "tsuba/FileFrame.h"
#include "tsuba/RDG.h"
#include "tsuba/tsuba.h"
#include "tsuba/RDG_internal.h"

/* This test tests the correctness of LoadPartialTable against the ground truth
 * of slicing the desired portion out of the original table.
 */

// TODO (scober): This would run faster if the input was part of `make inputs`
// (but probably still not fast enough for the 'quick' label).

namespace fs = boost::filesystem;

namespace {

const int64_t BIG_ARRAY_SIZE = 1 << 27;
const std::string TEST_DIR   = "/tmp/partial-load";

// Schema
std::shared_ptr<arrow::Schema> int64_schema() {
  auto field = std::make_shared<arrow::Field>(
      "test", std::make_shared<arrow::Int64Type>());
  auto schema =
      std::make_shared<arrow::Schema>(arrow::Schema({field}, nullptr));
  return schema;
}

// Table
std::shared_ptr<arrow::Table> big_table() {
  arrow::Int64Builder builder;
  arrow::Status status;

  for (int64_t i = 0; i < BIG_ARRAY_SIZE; ++i) {
    status = builder.Append(i * i);
    GALOIS_LOG_ASSERT(status.ok());
  }

  std::shared_ptr<arrow::Int64Array> arr;
  status = builder.Finish(&arr);
  GALOIS_LOG_ASSERT(status.ok());
  std::shared_ptr<arrow::Table> tab = arrow::Table::Make(int64_schema(), {arr});
  return tab;
}

// Test
void WriteInit(std::shared_ptr<arrow::Table> table, std::string path) {
  auto ff  = std::make_shared<tsuba::FileFrame>();
  auto res = ff->Init();
  GALOIS_LOG_ASSERT(res);

  auto write_result =
      parquet::arrow::WriteTable(*table, arrow::default_memory_pool(), ff,
                                 std::numeric_limits<int64_t>::max());
  GALOIS_LOG_ASSERT(write_result.ok());

  ff->Bind(path);
  res = ff->Persist();
  GALOIS_LOG_ASSERT(res);
}

void Test(std::shared_ptr<arrow::Table> table, std::string path, int64_t offset,
          int64_t length) {
  GALOIS_LOG_ASSERT(length >= 0 && offset >= 0);

  auto recovered_result =
      tsuba::internal::LoadPartialTable("test", path, offset, length);
  if (!recovered_result) {
    GALOIS_LOG_FATAL("tsuba::LoadPartialTable(\"test\", {}, {}, {}): {}", path,
                     offset, length, recovered_result.error());
  }
  std::shared_ptr<arrow::Table> recovered = recovered_result.value();

  GALOIS_LOG_ASSERT(recovered->Equals(*(table->Slice(offset, length))));
}
} // namespace

int main() {
  std::shared_ptr<arrow::Table> table = big_table();

  if (auto res = tsuba::Init(); !res) {
    GALOIS_LOG_FATAL("tsuba::Init: {}", res.error());
  }

  auto unique_result = galois::CreateUniqueDirectory(TEST_DIR);
  if (!unique_result) {
    GALOIS_LOG_FATAL("galois:CreateUniqueDirectory: {}", unique_result.error());
  }
  std::string temp_dir(std::move(unique_result.value()));

  auto path_result = galois::NewPath(temp_dir, "big_parquet");
  if (!path_result) {
    GALOIS_LOG_FATAL("galois::NewPath({}, \"{}\"): {}", temp_dir, "big_parquet",
                     path_result.error());
  }
  std::string path = path_result.value();
  WriteInit(table, path);

  // Run several tests
  std::random_device rd;
  std::mt19937 gen(rd());
  std::uniform_int_distribution<int64_t> dist(0, table->num_rows());

  Test(table, path, 0, 0);
  Test(table, path, 0, 888);
  Test(table, path, 8, table->num_rows() + 8);
  Test(table, path, dist(gen), dist(gen));
  Test(table, path, dist(gen), dist(gen));

  fs::remove_all(temp_dir);

  if (auto res = tsuba::Fini(); !res) {
    GALOIS_LOG_FATAL("tusba::Fini: {}", res.error());
  }

  return 0;
}
