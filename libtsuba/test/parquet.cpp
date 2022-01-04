#include <arrow/chunked_array.h>
#include <arrow/type_fwd.h>

#include "katana/ParquetReader.h"
#include "katana/ParquetWriter.h"
#include "katana/Result.h"
#include "katana/tsuba.h"

katana::Result<std::shared_ptr<arrow::ChunkedArray>>
MakeArrayOfStrings() {
  arrow::LargeStringBuilder builder;
  for (int i = 0; i < 100; ++i) {
    KATANA_CHECKED(builder.Append(fmt::format("test-string-row-{}", i)));
  }

  std::shared_ptr<arrow::Array> array;
  KATANA_CHECKED(builder.Finish(&array));
  return std::make_shared<arrow::ChunkedArray>(array);
}

katana::Result<void>
TestLargeStringRoundTrip(const std::string& dir) {
  auto uri =
      KATANA_CHECKED(katana::Uri::Make(dir)).Join("large_string.parquet");

  auto string_array = KATANA_CHECKED(MakeArrayOfStrings());
  auto writer =
      KATANA_CHECKED(katana::ParquetWriter::Make(string_array, "test-array"));

  KATANA_CHECKED(writer->WriteToUri(uri));

  auto reader = KATANA_CHECKED(katana::ParquetReader::Make());
  auto table = KATANA_CHECKED(reader->ReadTable(uri));

  KATANA_LOG_ASSERT(table->num_columns() == 1);
  KATANA_LOG_ASSERT(table->columns()[0]->type()->Equals(arrow::large_utf8()));

  return katana::ResultSuccess();
}

katana::Result<void>
TestAll(const std::string& dir) {
  KATANA_CHECKED_CONTEXT(
      TestLargeStringRoundTrip(dir), "TestLargeStringRoundTrip");

  return katana::ResultSuccess();
}

int
main(int argc, char* argv[]) {
  if (auto init_good = katana::InitTsuba(); !init_good) {
    KATANA_LOG_FATAL("katana::InitTsuba: {}", init_good.error());
  }

  if (argc <= 1) {
    KATANA_LOG_FATAL("{} <empty dir>", argv[0]);
  }

  auto res = TestAll(argv[1]);
  if (!res) {
    KATANA_LOG_FATAL("test failed: {}", res.error());
  }

  if (auto fini_good = katana::FiniTsuba(); !fini_good) {
    KATANA_LOG_FATAL("katana::FiniTsuba: {}", fini_good.error());
  }

  return 0;
}
