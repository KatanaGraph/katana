#include "katana/ArrowInterchange.h"

#include <numeric>

#include "katana/Random.h"

namespace {
std::shared_ptr<arrow::ChunkedArray>
IndexedTake(
    const std::shared_ptr<arrow::ChunkedArray>& original,
    std::shared_ptr<arrow::Array> indices) {
  // Use Take to select those indices
  arrow::Result<arrow::Datum> take_result =
      arrow::compute::Take(original, indices);
  KATANA_LOG_ASSERT(take_result.ok());
  arrow::Datum take = std::move(take_result.ValueOrDie());
  std::shared_ptr<arrow::ChunkedArray> chunked = take.chunked_array();
  KATANA_LOG_ASSERT(chunked->num_chunks() == 1);
  return chunked;
}

}  // anonymous namespace

std::shared_ptr<arrow::Array>
katana::Unchunk(const std::shared_ptr<arrow::ChunkedArray>& original) {
  int64_t length = original->length();
  // Build indices array, reusable across properties
  arrow::CTypeTraits<int64_t>::BuilderType builder;
  auto res = builder.Reserve(length);
  KATANA_LOG_ASSERT(res.ok());
  for (int64_t i = 0; i < length; ++i) {
    builder.UnsafeAppend(i);
  }
  std::shared_ptr<arrow::Array> indices;
  res = builder.Finish(&indices);
  KATANA_LOG_ASSERT(res.ok());
  auto chunked = IndexedTake(original, indices);
  return chunked->chunk(0);
}

std::shared_ptr<arrow::ChunkedArray>
katana::Shuffle(const std::shared_ptr<arrow::ChunkedArray>& original) {
  int64_t length = original->length();
  // Build indices array, reusable across properties
  std::vector<uint64_t> indices_vec(length);
  // fills the vector from 0 to indices_vec.size()-1
  std::iota(indices_vec.begin(), indices_vec.end(), 0);
  std::shuffle(indices_vec.begin(), indices_vec.end(), katana::GetGenerator());
  arrow::CTypeTraits<int64_t>::BuilderType builder;
  auto res = builder.Reserve(length);
  KATANA_LOG_ASSERT(res.ok());
  for (int64_t i = 0; i < length; ++i) {
    builder.UnsafeAppend(indices_vec[i]);
  }
  std::shared_ptr<arrow::Array> indices;
  res = builder.Finish(&indices);
  KATANA_LOG_ASSERT(res.ok());
  return IndexedTake(original, indices);
}
