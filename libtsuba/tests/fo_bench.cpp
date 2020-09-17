#include <time.h>

#include <chrono>
#include <thread>

#include "arrow/api.h"
#include "bench_utils.h"
#include "galois/FileSystem.h"
#include "galois/Logging.h"
#include "galois/Random.h"
#include "parquet/file_reader.h"
#include "tsuba/Errors.h"
#include "tsuba/FileFrame.h"
#include "tsuba/FileView.h"
#include "tsuba/file.h"
#include "tsuba/tsuba.h"

/* This file is intended to benchmark changes in our arrow parquet translation.
 * *ArrLib functions represent the very first draft of that translation.
 *
 * ministat is a nice tool to process the data.
 */

namespace {

// The constant from which (nearly) all tables derive their size
const int64_t BIG_ARRAY_SIZE = 1 << 27;

// For time conversions (all benchmarks in this file attempt to run for an
// amount of time that can be meaningfully represented in seconds
const double NANO = 1000000000;

// Path forming stuff
const std::string s3_base = "s3://simon-test-useast2/";
const std::string local_base = "/tmp/";

// Output formatting
const std::string no_measurement = "-------";
const std::string indent = "  ";

// Utilities
// TODO (scober) fmt::println equivalent
std::string
timing_string() {
  return fmt::format(
      "{},{},{}", no_measurement, no_measurement, no_measurement);
}

std::string
timing_string(struct timespec before, struct timespec after) {
  struct timespec total = timespec_sub(after, before);
  return fmt::format(
      "{},{},{}", total.tv_sec + total.tv_nsec / NANO, no_measurement,
      no_measurement);
}

std::string
write_timing_string(
    struct timespec before, struct timespec middle, struct timespec after) {
  struct timespec total = timespec_sub(after, before);
  struct timespec first = timespec_sub(middle, before);
  struct timespec second = timespec_sub(after, middle);
  return fmt::format(
      "{},{},{}", total.tv_sec + total.tv_nsec / NANO,
      first.tv_sec + first.tv_nsec / NANO,
      second.tv_sec + second.tv_nsec / NANO);
}

std::string
read_timing_string(
    struct timespec before, struct timespec middle, struct timespec after) {
  struct timespec total = timespec_sub(after, before);
  struct timespec first = timespec_sub(middle, before);
  struct timespec second = timespec_sub(after, middle);
  return fmt::format(
      "{},{},{}", total.tv_sec + total.tv_nsec / NANO,
      second.tv_sec + second.tv_nsec / NANO,
      first.tv_sec + first.tv_nsec / NANO);
}

// Schemas
std::shared_ptr<arrow::Schema>
int64_schema() {
  auto field = std::make_shared<arrow::Field>(
      "test", std::make_shared<arrow::Int64Type>());
  auto schema =
      std::make_shared<arrow::Schema>(arrow::Schema({field}, nullptr));
  return schema;
}

std::shared_ptr<arrow::Schema>
int32_schema() {
  auto field = std::make_shared<arrow::Field>(
      "test", std::make_shared<arrow::Int32Type>());
  auto schema = std::make_shared<arrow::Schema>(arrow::Schema({field}));
  return schema;
}

std::shared_ptr<arrow::Schema>
string_schema() {
  auto field = std::make_shared<arrow::Field>(
      "test", std::make_shared<arrow::StringType>());
  auto schema = std::make_shared<arrow::Schema>(arrow::Schema({field}));
  return schema;
}

// Tables
std::shared_ptr<arrow::Table>
repeated_string_table() {
  arrow::StringBuilder builder;
  arrow::Status status;

  for (int64_t i = 0; i < BIG_ARRAY_SIZE / 4; ++i) {
    status = builder.Append("The Katana Graph Engine is an absolute banger.");
    GALOIS_LOG_ASSERT(status.ok());
  }

  std::shared_ptr<arrow::StringArray> arr;
  status = builder.Finish(&arr);
  GALOIS_LOG_ASSERT(status.ok());
  std::shared_ptr<arrow::Table> tab =
      arrow::Table::Make(string_schema(), {arr});
  return tab;
}

std::shared_ptr<arrow::Table>
big_table() {
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

std::shared_ptr<arrow::Table>
huge_table() {
  arrow::Int64Builder builder;
  arrow::Status status;

  for (int64_t i = 0; i < BIG_ARRAY_SIZE * 4; ++i) {
    status = builder.Append(i * i);
    GALOIS_LOG_ASSERT(status.ok());
  }

  std::shared_ptr<arrow::Int64Array> arr;
  status = builder.Finish(&arr);
  GALOIS_LOG_ASSERT(status.ok());
  std::shared_ptr<arrow::Table> tab = arrow::Table::Make(int64_schema(), {arr});
  return tab;
}

std::shared_ptr<arrow::Table>
huger_table() {
  arrow::Int64Builder builder;
  arrow::Status status;

  for (int64_t i = 0; i < BIG_ARRAY_SIZE * 16; ++i) {
    status = builder.Append(i * i);
    GALOIS_LOG_ASSERT(status.ok());
  }

  std::shared_ptr<arrow::Int64Array> arr;
  status = builder.Finish(&arr);
  GALOIS_LOG_ASSERT(status.ok());
  std::shared_ptr<arrow::Table> tab = arrow::Table::Make(int64_schema(), {arr});
  return tab;
}

std::shared_ptr<arrow::Table>
small_table() {
  arrow::Int32Builder builder;
  arrow::Status status;

  for (int32_t i = 0; i < BIG_ARRAY_SIZE; ++i) {
    status = builder.Append(i * i);
    GALOIS_LOG_ASSERT(status.ok());
  }

  std::shared_ptr<arrow::Int32Array> arr;
  status = builder.Finish(&arr);
  GALOIS_LOG_ASSERT(status.ok());
  std::shared_ptr<arrow::Table> tab = arrow::Table::Make(int32_schema(), {arr});
  return tab;
}

std::shared_ptr<arrow::Table>
small2_table() {
  arrow::Int64Builder builder;
  arrow::Status status;

  for (int64_t i = 0; i < BIG_ARRAY_SIZE / 2; ++i) {
    status = builder.Append(i * i);
    GALOIS_LOG_ASSERT(status.ok());
  }

  std::shared_ptr<arrow::Int64Array> arr;
  status = builder.Finish(&arr);
  GALOIS_LOG_ASSERT(status.ok());
  std::shared_ptr<arrow::Table> tab = arrow::Table::Make(int64_schema(), {arr});
  return tab;
}

std::shared_ptr<arrow::Table>
speckled_table() {
  arrow::Int64Builder builder;
  arrow::Status status;

  for (int64_t i = 0; i < BIG_ARRAY_SIZE; i += 2) {
    status = builder.Append(i * i);
    GALOIS_LOG_ASSERT(status.ok());
    status = builder.AppendNull();
    GALOIS_LOG_ASSERT(status.ok());
  }

  std::shared_ptr<arrow::Int64Array> arr;
  status = builder.Finish(&arr);
  GALOIS_LOG_ASSERT(status.ok());
  std::shared_ptr<arrow::Table> tab = arrow::Table::Make(int64_schema(), {arr});
  return tab;
}

std::shared_ptr<arrow::Table>
super_void_table() {
  arrow::Int64Builder builder;
  arrow::Status status;

  status = builder.Append(0);
  GALOIS_LOG_ASSERT(status.ok());
  for (int64_t i = 0; i < BIG_ARRAY_SIZE - 2; ++i) {
    status = builder.AppendNull();
    GALOIS_LOG_ASSERT(status.ok());
  }
  status = builder.Append(1);
  GALOIS_LOG_ASSERT(status.ok());

  std::shared_ptr<arrow::Int64Array> arr;
  status = builder.Finish(&arr);
  GALOIS_LOG_ASSERT(status.ok());
  std::shared_ptr<arrow::Table> tab = arrow::Table::Make(int64_schema(), {arr});
  return tab;
}

std::shared_ptr<arrow::Table>
please_compress_table() {
  arrow::Int64Builder builder;
  arrow::Status status;

  status = builder.Append(0);
  GALOIS_LOG_ASSERT(status.ok());
  for (int64_t i = 0; i < BIG_ARRAY_SIZE - 2; ++i) {
    status = builder.Append(34);
    GALOIS_LOG_ASSERT(status.ok());
  }
  status = builder.Append(1);
  GALOIS_LOG_ASSERT(status.ok());

  std::shared_ptr<arrow::Int64Array> arr;
  status = builder.Finish(&arr);
  GALOIS_LOG_ASSERT(status.ok());
  std::shared_ptr<arrow::Table> tab = arrow::Table::Make(int64_schema(), {arr});
  return tab;
}

// Benchmarks
void
WriteArrLib(
    std::shared_ptr<arrow::Table> table, std::string path, std::FILE* stream) {
  struct timespec start;
  struct timespec middle;
  struct timespec end;
  start = now();

  auto create_result = arrow::io::BufferOutputStream::Create();
  GALOIS_LOG_ASSERT(create_result.ok());

  std::shared_ptr<arrow::io::BufferOutputStream> out =
      create_result.ValueOrDie();
  auto write_result = parquet::arrow::WriteTable(
      *table, arrow::default_memory_pool(), out,
      std::numeric_limits<int64_t>::max());
  GALOIS_LOG_ASSERT(write_result.ok());

  auto finish_result = out->Finish();
  GALOIS_LOG_ASSERT(finish_result.ok());

  std::shared_ptr<arrow::Buffer> buf = finish_result.ValueOrDie();

  middle = now();

  auto res = tsuba::FileStore(path, buf->data(), buf->size());
  GALOIS_LOG_ASSERT(res);

  end = now();

  fmt::print(
      stream, "Arrow_Library_Write,{},{}\n", path,
      write_timing_string(start, middle, end));
}

// SCB July.13.2020 Arrow S3 support is difficult to use. It is not compiled
// into the 0.17.1 apt package, and I couldn't get it to work even after I built
// libarrow from source with S3 enabled.
//
//  std::shared_ptr<arrow::Table>
// ReadArrLib(std::string path, std::FILE* stream) {
//
//  struct timespec start;
//  struct timespec middle;
//  struct timespec end;
//
//  start = now();
//
//  arrow::Status init_stat = arrow::fs::EnsureS3Initialized();
//  GALOIS_LOG_ASSERT(init_stat.ok());
//  auto s3opt = arrow::fs::S3Options::Defaults();
//  //  s3opt.region   = "us-east-2";
//  auto fs_result = arrow::fs::S3FileSystem::Make(s3opt);
//  GALOIS_LOG_ASSERT(fs_result.ok());
//
//  std::shared_ptr<arrow::fs::S3FileSystem> s3fs = fs_result.ValueOrDie();
//  auto fs =
//  std::make_shared<arrow::fs::SubTreeFileSystem>("simon-test-useast2",
//                                                           s3fs);
//  auto open_result = fs->OpenInputFile(path);
//  GALOIS_LOG_ASSERT(open_result.ok());
//
//  std::shared_ptr<arrow::io::RandomAccessFile> f = open_result.ValueOrDie();
//  std::unique_ptr<parquet::arrow::FileReader> reader;
//  auto open_file_result =
//      parquet::arrow::OpenFile(f, arrow::default_memory_pool(), &reader);
//  GALOIS_LOG_ASSERT(open_file_result.ok());
//
//  middle = now();
//
//  std::shared_ptr<arrow::Table> out;
//  auto read_result = reader->ReadTable(&out);
//  GALOIS_LOG_ASSERT(read_result.ok());
//
//  arrow::Status status = arrow::fs::FinalizeS3();
//  GALOIS_LOG_ASSERT(status.ok());
//
//  end = now();
//
//  fmt::print(stream, "Arrow_Library_Read,{},{}\n", path,
//  read_timing_string(start, middle, end));
//
//  return out;
//}

void
ReadArrLib(std::string path, std::FILE* stream) {
  fmt::print(stream, "Arrow_Library_Read,{},{}\n", path, timing_string());
}

void
WriteFF(
    std::shared_ptr<arrow::Table> table, std::string path, std::FILE* stream) {
  struct timespec start;
  struct timespec middle;
  struct timespec end;
  start = now();

  auto ff = std::make_shared<tsuba::FileFrame>();
  auto res = ff->Init();
  GALOIS_LOG_ASSERT(res);

  auto write_result = parquet::arrow::WriteTable(
      *table, arrow::default_memory_pool(), ff,
      std::numeric_limits<int64_t>::max());
  if (!write_result.ok()) {
    GALOIS_LOG_FATAL(
        "WriteTable failed with path {} and error {}", path, write_result);
  }

  middle = now();

  ff->Bind(path);
  if (res = ff->Persist(); !res) {
    GALOIS_LOG_FATAL(
        "FileFrame::Persist failed. path={}, result={}", path, res.error());
  }

  end = now();

  fmt::print(
      stream, "FileFrame::Write,{},{}\n", path,
      write_timing_string(start, middle, end));
}

std::shared_ptr<arrow::Table>
ReadFV_v0(std::string path, std::FILE* stream) {
  struct timespec start;
  struct timespec middle;
  struct timespec end;

  start = now();

  auto fv = std::make_shared<tsuba::FileView>();
  auto res = fv->Bind(path, true);
  GALOIS_LOG_ASSERT(res);

  middle = now();

  std::unique_ptr<parquet::arrow::FileReader> reader;
  auto open_file_result =
      parquet::arrow::OpenFile(fv, arrow::default_memory_pool(), &reader);
  GALOIS_LOG_ASSERT(open_file_result.ok());

  std::shared_ptr<arrow::Table> out;
  auto read_result = reader->ReadTable(&out);
  GALOIS_LOG_ASSERT(read_result.ok());

  end = now();

  fmt::print(
      stream, "FileView::Read(v0),{},{}\n", path,
      read_timing_string(start, middle, end));
  return out;
}

std::shared_ptr<arrow::Table>
ReadFV_v0_1(std::string path, std::FILE* stream) {
  struct timespec start;
  struct timespec middle;
  struct timespec end;

  start = now();

  auto fv = std::make_shared<tsuba::FileView>();
  auto res = fv->Bind(path, 0, 0, true);
  GALOIS_LOG_ASSERT(res);

  // Read in the entire file, one row group at a time
  std::unique_ptr<parquet::arrow::FileReader> reader;
  auto open_file_result =
      parquet::arrow::OpenFile(fv, arrow::default_memory_pool(), &reader);
  GALOIS_LOG_ASSERT(open_file_result.ok());

  auto aro_sts = fv->Seek(0);
  GALOIS_LOG_ASSERT(aro_sts.ok());

  int rg_count = reader->num_row_groups();
  arrow::Result<std::shared_ptr<arrow::Buffer>> aro_res;
  for (int i = 0; i < rg_count; ++i) {
    aro_res = fv->Read(
        reader->parquet_reader()->metadata()->RowGroup(i)->total_byte_size());
    GALOIS_LOG_ASSERT(aro_res.ok());
  }

  middle = now();

  std::shared_ptr<arrow::Table> out;
  auto read_result = reader->ReadTable(&out);
  GALOIS_LOG_ASSERT(read_result.ok());

  end = now();

  fmt::print(
      stream, "FileView::Read(v0.1),{},{}\n", path,
      read_timing_string(start, middle, end));
  return out;
}

std::shared_ptr<arrow::Table>
ReadFV_v1(std::string path, std::FILE* stream) {
  struct timespec start;
  struct timespec end;

  start = now();

  auto fv = std::make_shared<tsuba::FileView>();
  auto res = fv->Bind(path, 0, 0, true);
  GALOIS_LOG_ASSERT(res);

  std::unique_ptr<parquet::arrow::FileReader> reader;
  auto open_file_result =
      parquet::arrow::OpenFile(fv, arrow::default_memory_pool(), &reader);
  GALOIS_LOG_ASSERT(open_file_result.ok());

  std::shared_ptr<arrow::Table> out;
  auto read_result = reader->ReadTable(&out);
  if (!read_result.ok()) {
    GALOIS_LOG_ERROR(
        "ReadTable failed on path {} with error {}", path, read_result);
  }

  end = now();

  fmt::print(
      stream, "FileView::Read(v1),{},{}\n", path, timing_string(start, end));
  return out;
}

// Do a whole bunch of reads to try to measure the overhead of various Read
// implementations
void
ReadOverheadFV(std::string path, std::FILE* stream) {
  struct timespec start;
  struct timespec end;

  auto fv = std::make_shared<tsuba::FileView>();
  auto res = fv->Bind(path, true);
  GALOIS_LOG_ASSERT(res);

  start = now();

  arrow::Result<std::shared_ptr<arrow::Buffer>> buf;
  arrow::Result<int64_t> size_result = fv->GetSize();
  GALOIS_LOG_ASSERT(size_result.ok());
  int64_t file_size = size_result.ValueOrDie();

  arrow::Status sts;

  for (int64_t bytes = 1; bytes < file_size; bytes *= 2) {
    for (int i = 0; i < 1000; ++i) {
      sts = fv->Seek(0);
      GALOIS_LOG_ASSERT(sts.ok());
      buf = fv->Read(bytes);
    }
  }

  end = now();

  fmt::print(
      stream, "FileView::Read(overhead),{},{}\n", path,
      timing_string(start, end));
}

// Read whole file into memory, then convert only the needed row groups
std::shared_ptr<arrow::Table>
ReadPartial_v0(
    std::string path, int64_t offset, int64_t length, std::FILE* stream) {
  GALOIS_LOG_ASSERT(offset >= 0 && length >= 0);

  struct timespec start;
  struct timespec middle;
  struct timespec end;

  start = now();

  auto fv = std::make_shared<tsuba::FileView>(tsuba::FileView());
  auto res = fv->Bind(path, true);
  GALOIS_LOG_ASSERT(res);

  middle = now();

  std::unique_ptr<parquet::arrow::FileReader> reader;

  auto open_file_result =
      parquet::arrow::OpenFile(fv, arrow::default_memory_pool(), &reader);
  GALOIS_LOG_ASSERT(open_file_result.ok());

  std::shared_ptr<arrow::Table> out;
  std::shared_ptr<arrow::Table> ret;
  auto read_result = reader->ReadTable(&out);
  GALOIS_LOG_ASSERT(read_result.ok());
  ret = out->Slice(offset, length);

  end = now();

  fmt::print(
      stream, "FileView_PartialRead(v0),{},{}\n", path,
      read_timing_string(start, middle, end));
  return ret;
}

// Read only needed row groups into memory, synchronously
std::shared_ptr<arrow::Table>
ReadPartial_v1(
    std::string path, int64_t offset, int64_t length, std::FILE* stream) {
  GALOIS_LOG_ASSERT(offset >= 0 && length >= 0);

  struct timespec start;
  struct timespec middle;
  struct timespec end;

  start = now();

  auto fv = std::make_shared<tsuba::FileView>(tsuba::FileView());
  auto res = fv->Bind(path, true);
  GALOIS_LOG_ASSERT(res);

  middle = now();

  std::unique_ptr<parquet::arrow::FileReader> reader;

  auto open_file_result =
      parquet::arrow::OpenFile(fv, arrow::default_memory_pool(), &reader);
  GALOIS_LOG_ASSERT(open_file_result.ok());

  std::vector<int> row_groups;
  int rg_count = reader->num_row_groups();
  int64_t internal_offset = 0;
  int64_t cumulative_rows = 0;
  for (int i = 0; cumulative_rows < offset + length && i < rg_count; ++i) {
    int64_t new_rows =
        reader->parquet_reader()->metadata()->RowGroup(i)->num_rows();
    if (offset < cumulative_rows + new_rows) {
      if (row_groups.empty()) {
        internal_offset = offset - cumulative_rows;
        GALOIS_LOG_ASSERT(internal_offset >= 0);
      }
      row_groups.push_back(i);
    }
    cumulative_rows += new_rows;
  }

  std::shared_ptr<arrow::Table> out;
  std::shared_ptr<arrow::Table> ret;
  auto read_result = reader->ReadRowGroups(row_groups, &out);
  GALOIS_LOG_ASSERT(read_result.ok());
  ret = out->Slice(internal_offset, length);

  end = now();

  fmt::print(
      stream, "FileView_PartialRead(v1),{},{}\n", path,
      read_timing_string(start, middle, end));
  return ret;
}

// Read only necessary row groups into memory, but allow FileView to handle this
// asynchronously
std::shared_ptr<arrow::Table>
ReadPartial_v2(
    std::string path, int64_t offset, int64_t length, std::FILE* stream) {
  GALOIS_LOG_ASSERT(offset >= 0 && length >= 0);

  struct timespec start;
  struct timespec middle;
  struct timespec end;

  start = now();

  auto fv = std::make_shared<tsuba::FileView>(tsuba::FileView());
  auto res = fv->Bind(path, 0, 0, true);
  GALOIS_LOG_ASSERT(res);

  std::unique_ptr<parquet::arrow::FileReader> reader;

  auto open_file_result =
      parquet::arrow::OpenFile(fv, arrow::default_memory_pool(), &reader);
  GALOIS_LOG_ASSERT(open_file_result.ok());

  std::vector<int> row_groups;
  int rg_count = reader->num_row_groups();
  int64_t row_offset = 0;
  int64_t cumulative_rows = 0;
  int64_t file_offset = 0;
  int64_t cumulative_bytes = 0;
  for (int i = 0; cumulative_rows < offset + length && i < rg_count; ++i) {
    auto rg_md = reader->parquet_reader()->metadata()->RowGroup(i);
    int64_t new_rows = rg_md->num_rows();
    int64_t new_bytes = rg_md->total_byte_size();
    if (offset < cumulative_rows + new_rows) {
      if (row_groups.empty()) {
        row_offset = offset - cumulative_rows;
        assert(row_offset >= 0);
        file_offset = cumulative_bytes;
      }
      row_groups.push_back(i);
    }
    cumulative_rows += new_rows;
    cumulative_bytes += new_bytes;
  }

  res = fv->Fill(file_offset, cumulative_bytes, true);
  GALOIS_LOG_ASSERT(res);

  middle = now();

  std::shared_ptr<arrow::Table> out;
  std::shared_ptr<arrow::Table> ret;
  auto read_result = reader->ReadRowGroups(row_groups, &out);
  GALOIS_LOG_ASSERT(read_result.ok());
  ret = out->Slice(row_offset, length);

  end = now();

  fmt::print(
      stream, "FileView_PartialRead(v2),{},{}\n", path,
      read_timing_string(start, middle, end));
  return ret;
}

void
ReadMetaFV(std::string path, std::FILE*) {
  fmt::print("path: {}\n", path);
  auto fv = std::make_shared<tsuba::FileView>(tsuba::FileView());
  auto res = fv->Bind(path, 0, 0, true);
  GALOIS_LOG_ASSERT(res);

  std::unique_ptr<parquet::arrow::FileReader> reader;

  auto open_file_result =
      parquet::arrow::OpenFile(fv, arrow::default_memory_pool(), &reader);
  GALOIS_LOG_ASSERT(open_file_result.ok());

  auto file_meta = reader->parquet_reader()->metadata();
  fmt::print("file siz\ne: {}", fv->size());
  fmt::print("metadata siz\ne: {}", file_meta->size());
  int rg_count = reader->num_row_groups();
  for (int i = 0; i < rg_count; ++i) {
    auto rg_md = file_meta->RowGroup(i);
    fmt::print("{}rowgroup {}\n", indent, i);
    fmt::print("{}{}number \nof rows : {}", indent, indent, rg_md->num_rows());
    fmt::print(
        "{}{}number \nof bytes: {}", indent, indent, rg_md->total_byte_size());
  }
}

typedef std::shared_ptr<arrow::Table> table_maker();

struct table_info {
  const char* name;
  bool dump_meta;
  uint8_t alw_count;   // libarrow write
  uint8_t alr_count;   // libarrow read
  uint8_t ffw_count;   // FileFrame write
  uint8_t fvo_count;   // FileView read overhead
  uint8_t fvr0_count;  // FileView read v0 (synchronous read then convert)
  uint8_t
      fvr0_1_count;     // FileView read v0.1 (synchronous read one row group at
                        // a time)
  uint8_t fvr1_count;   // FileView read v1
  uint8_t fvpr0_count;  // FileView partial read (v0)
  uint8_t fvpr1_count;  // FileView partial read (v1)
  uint8_t fvpr2_count;  // FileView partial read (v2)
  table_maker* table;
};

table_info exps[] = {
    {"sml", false, 0, 0, 3, 0, 3, 0, 3, 0, 0, 0, &small_table},
    {"sm2", false, 0, 0, 3, 0, 3, 0, 3, 0, 0, 0, &small2_table},
    {"big", false, 0, 0, 3, 0, 3, 0, 3, 0, 0, 0, &big_table},
    {"hug", false, 0, 0, 3, 0, 3, 0, 3, 0, 0, 0, &huge_table},
    {"hgr", false, 0, 0, 3, 0, 3, 0, 3, 0, 0, 0, &huger_table},
    {"spd", false, 0, 0, 3, 0, 3, 0, 3, 0, 0, 0, &speckled_table},
    {"svd", false, 0, 0, 3, 0, 3, 0, 3, 0, 0, 0, &super_void_table},
    {"cmp", false, 0, 0, 3, 0, 3, 0, 3, 0, 0, 0, &please_compress_table},
    {"str", false, 0, 0, 3, 0, 3, 0, 3, 0, 0, 0, &repeated_string_table},
};

}  // namespace

int
main() {
  // We want to run some tests with no output
  std::FILE* fnull = fopen("/dev/null", "w");
  if (fnull == nullptr) {
    GALOIS_LOG_ERROR(
        "Opening /dev/null failed with error '{}'. Will print all "
        "garbage to stderr.",
        std::strerror(errno));
    fnull = stderr;
  }

  fmt::print("method,file,total,memory,persistent\n");

  // TODO (scober): Can we set the S3 region from here?
  galois::Result<void> result = tsuba::Init();
  GALOIS_LOG_ASSERT(result);
  for (table_info t_info : exps) {
    const std::string path = s3_base + t_info.name;
    int64_t offset = 0;
    int64_t partial_length = 0;
    std::shared_ptr<arrow::Table> table;
    std::shared_ptr<arrow::Table> recovered;
    if (t_info.alw_count > 0 || t_info.alr_count > 0 || t_info.ffw_count > 0 ||
        t_info.fvo_count > 0 || t_info.fvr0_count > 0 ||
        t_info.fvr0_1_count > 0 || t_info.fvr1_count > 0 ||
        t_info.fvpr0_count > 0 || t_info.fvpr1_count > 0 ||
        t_info.fvpr2_count > 0 || t_info.dump_meta) {
      table = (*t_info.table)();
      partial_length = table->num_rows() / 3;

      if (t_info.alw_count == 0 && t_info.ffw_count == 0) {
        // TODO(scober): Do a real 'file exists' check (implement first)
        tsuba::StatBuf buf;
        if (auto res = tsuba::FileStat(path, &buf); !res) {
          // We need to run at least one test but no write tests were requested
          // and here we assume that the file in question does not exist
          WriteFF(table, path, fnull);
        }
      }
    }

    // Run all requested tests
    for (uint8_t i = 0; i < t_info.alw_count; ++i) {
      WriteArrLib(table, path, stdout);
    }
    for (uint8_t i = 0; i < t_info.ffw_count; ++i) {
      WriteFF(table, path, stdout);
    }
    if (t_info.dump_meta) {
      ReadMetaFV(path, stderr);
    }
    for (uint8_t i = 0; i < t_info.fvo_count; ++i) {
      ReadOverheadFV(path, stdout);
    }
    for (uint8_t i = 0; i < t_info.alr_count; ++i) {
      ReadArrLib(path, stdout);
    }
    for (uint8_t i = 0; i < t_info.fvr0_count; ++i) {
      recovered = ReadFV_v0(path, stdout);
      GALOIS_LOG_ASSERT(recovered->Equals(*table));
    }
    for (uint8_t i = 0; i < t_info.fvr0_1_count; ++i) {
      recovered = ReadFV_v0_1(path, stdout);
      GALOIS_LOG_ASSERT(recovered->Equals(*table));
    }
    for (uint8_t i = 0; i < t_info.fvr1_count; ++i) {
      recovered = ReadFV_v1(path, stdout);
      GALOIS_LOG_ASSERT(recovered->Equals(*table));
    }
    for (uint8_t i = 0; i < t_info.fvpr0_count; ++i) {
      offset = galois::RandomUniformInt(table->num_rows() - partial_length);
      recovered = ReadPartial_v0(path, offset, partial_length, stdout);
      GALOIS_LOG_ASSERT(
          recovered->Equals(*(table->Slice(offset, partial_length))));
    }
    for (uint8_t i = 0; i < t_info.fvpr1_count; ++i) {
      offset = galois::RandomUniformInt(table->num_rows() - partial_length);
      recovered = ReadPartial_v1(path, offset, partial_length, stdout);
      GALOIS_LOG_ASSERT(
          recovered->Equals(*(table->Slice(offset, partial_length))));
    }
    for (uint8_t i = 0; i < t_info.fvpr2_count; ++i) {
      offset = galois::RandomUniformInt(table->num_rows() - partial_length);
      recovered = ReadPartial_v2(path, offset, partial_length, stdout);
      GALOIS_LOG_ASSERT(
          recovered->Equals(*(table->Slice(offset, partial_length))));
    }
  }

  if (fnull != stderr) {
    fclose(fnull);
  }

  result = tsuba::Fini();
  GALOIS_LOG_ASSERT(result);

  return 0;
}
