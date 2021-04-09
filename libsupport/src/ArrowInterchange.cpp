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

std::shared_ptr<arrow::Array>
Indices(const std::shared_ptr<arrow::ChunkedArray>& original) {
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
  return indices;
}

}  // anonymous namespace

katana::Result<std::shared_ptr<arrow::ChunkedArray>>
katana::UpdateChunkedArray(
    const std::shared_ptr<arrow::ChunkedArray>& chunka,
    const std::shared_ptr<arrow::Scalar>& scalar, int64_t position) {
  auto before = chunka->Slice(0, position);
  std::shared_ptr<arrow::ChunkedArray> after;
  if (position == chunka->length()) {
    after = EmptyChunkedArray(chunka->type(), 0);
  } else {
    after = chunka->Slice(position + 1, chunka->length() - position);
  }
  std::vector<std::shared_ptr<arrow::Array>> chunks;
  for (int32_t i = 0; i < before->num_chunks(); ++i) {
    chunks.push_back(before->chunk(i));
  }
  auto maybe_array = arrow::MakeArrayFromScalar(*scalar, 1);
  if (!maybe_array.ok()) {
    return KATANA_ERROR(
        katana::ErrorCode::ArrowError, "adding scalar {} at position {}",
        scalar->ToString(), position);
  }
  chunks.push_back(maybe_array.ValueOrDie());
  for (int32_t i = 0; i < after->num_chunks(); ++i) {
    chunks.push_back(after->chunk(i));
  }
  return std::make_shared<arrow::ChunkedArray>(chunks);
}

std::shared_ptr<arrow::Array>
katana::Unchunk(const std::shared_ptr<arrow::ChunkedArray>& original) {
  auto indices = Indices(original);
  auto chunked = IndexedTake(original, indices);
  return chunked->chunk(0);
}

std::shared_ptr<arrow::ChunkedArray>
katana::Defragment(const std::shared_ptr<arrow::ChunkedArray>& original) {
  auto indices = Indices(original);
  return IndexedTake(original, indices);
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

std::shared_ptr<arrow::ChunkedArray>
katana::EmptyChunkedArray(
    const std::shared_ptr<arrow::DataType>& type, int64_t length) {
  auto maybe_array = arrow::MakeArrayOfNull(type, length);
  if (!maybe_array.ok()) {
    KATANA_LOG_ERROR(
        "cannot create an empty arrow array: {}", maybe_array.status());
    return nullptr;
  }
  std::vector<std::shared_ptr<arrow::Array>> chunks{maybe_array.ValueOrDie()};
  return std::make_shared<arrow::ChunkedArray>(chunks);
}

template <typename BuilderType, typename ScalarType>
katana::Result<std::shared_ptr<arrow::Array>>
BasicToArray(
    const std::shared_ptr<arrow::DataType>& data_type,
    const std::vector<arrow::Scalar>& data) {
  if (data.empty()) {
    return katana::ResultSuccess();
  }
  auto* pool = arrow::default_memory_pool();
  auto builder = std::make_shared<BuilderType>(data_type, pool);
  if (auto st = builder->Resize(data.size()); !st.ok()) {
    return KATANA_ERROR(
        katana::ErrorCode::ArrowError,
        "arrow builder failed resize: {} type: {} reason: {}", data.size(),
        data_type->name(), st);
  }
  for (const auto& scalar : data) {
    if (scalar.is_valid) {
      // Is there a better way?
      builder->UnsafeAppend(static_cast<const ScalarType*>(&scalar)->value);
    } else {
      builder->UnsafeAppendNull();
    }
  }
  std::shared_ptr<arrow::Array> array;
  auto res = builder->Finish(&array);
  if (!res.ok()) {
    return KATANA_ERROR(
        katana::ErrorCode::ArrowError, "arrow builder finish type: {} : {}",
        data_type->name(), res);
  }
  return array;
}

template <typename BuilderType>
katana::Result<std::shared_ptr<arrow::Array>>
StringLikeToArray(
    const std::shared_ptr<arrow::DataType>& data_type,
    const std::vector<arrow::Scalar>& data) {
  if (data.empty()) {
    return katana::ResultSuccess();
  }
  auto* pool = arrow::default_memory_pool();
  auto builder = std::make_shared<BuilderType>(data_type, pool);
  if (auto st = builder->Resize(data.size()); !st.ok()) {
    return KATANA_ERROR(
        katana::ErrorCode::ArrowError,
        "arrow builder failed resize: {} type: {} reason: {}", data.size(),
        data_type->name(), st);
  }
  for (const auto& scalar : data) {
    if (scalar.is_valid) {
      auto workaround = scalar.ToString();
      fmt::print(
          "type {} {} {}\n", data_type->name(), scalar.type->name(),
          workaround);
      //auto ss = scalar.CastTo(data_type);
      //KATANA_LOG_ASSERT(ss.ok());
      //      if (auto res = builder->Append(ss.ValueOrDie()->ToString()); !res.ok()) {
      if (auto res = builder->Append(workaround); !res.ok()) {
        return KATANA_ERROR(
            katana::ErrorCode::ArrowError,
            "arrow builder failed append type: {} : {}", scalar.type->name(),
            res.CodeAsString());
      }
    } else {
      if (auto res = builder->AppendNull(); !res.ok()) {
        return KATANA_ERROR(
            katana::ErrorCode::ArrowError,
            "arrow builder failed append null: {}", scalar.type->name(),
            res.CodeAsString());
      }
    }
  }
  std::shared_ptr<arrow::Array> array;
  auto res = builder->Finish(&array);
  if (!res.ok()) {
    return KATANA_ERROR(
        katana::ErrorCode::ArrowError, "arrow builder finish type: {} : {}",
        data_type->name(), res.CodeAsString());
  }
  return array;
}

katana::Result<std::shared_ptr<arrow::Array>>
katana::ScalarVecToArray(
    const std::shared_ptr<arrow::DataType>& data_type,
    const std::vector<arrow::Scalar>& data) {
  std::shared_ptr<arrow::Array> array;
  switch (data_type->id()) {
  case arrow::Type::LARGE_STRING: {
    return StringLikeToArray<arrow::LargeStringBuilder>(data_type, data);
    break;
  }
  case arrow::Type::STRING: {
    return StringLikeToArray<arrow::StringBuilder>(data_type, data);
    break;
  }
  case arrow::Type::INT64: {
    return BasicToArray<arrow::Int64Builder, arrow::Int64Scalar>(
        data_type, data);
    break;
  }
  case arrow::Type::UINT64: {
    return BasicToArray<arrow::UInt64Builder, arrow::UInt64Scalar>(
        data_type, data);
    break;
  }
  case arrow::Type::INT32: {
    return BasicToArray<arrow::Int32Builder, arrow::Int32Scalar>(
        data_type, data);
    break;
  }
  case arrow::Type::UINT32: {
    return BasicToArray<arrow::UInt32Builder, arrow::UInt32Scalar>(
        data_type, data);
    break;
  }
  case arrow::Type::INT16: {
    return BasicToArray<arrow::Int16Builder, arrow::Int16Scalar>(
        data_type, data);
    break;
  }
  case arrow::Type::UINT16: {
    return BasicToArray<arrow::UInt16Builder, arrow::UInt16Scalar>(
        data_type, data);
    break;
  }
  case arrow::Type::INT8: {
    return BasicToArray<arrow::Int8Builder, arrow::Int8Scalar>(data_type, data);
    break;
  }
  case arrow::Type::UINT8: {
    return BasicToArray<arrow::UInt8Builder, arrow::UInt8Scalar>(
        data_type, data);
    break;
  }
  case arrow::Type::DOUBLE: {
    return BasicToArray<arrow::DoubleBuilder, arrow::DoubleScalar>(
        data_type, data);
    break;
  }
  case arrow::Type::FLOAT: {
    return BasicToArray<arrow::FloatBuilder, arrow::FloatScalar>(
        data_type, data);
    break;
  }
  case arrow::Type::BOOL: {
    return BasicToArray<arrow::BooleanBuilder, arrow::BooleanScalar>(
        data_type, data);
    break;
  }
  case arrow::Type::TIMESTAMP: {
    return BasicToArray<arrow::TimestampBuilder, arrow::TimestampScalar>(
        data_type, data);
    break;
  }
  case arrow::Type::LIST: {
    return KATANA_ERROR(
        katana::ErrorCode::ArrowError, "list types not supported");
    break;
  }
  default: {
    break;
  }
  }
  return KATANA_ERROR(
      katana::ErrorCode::ArrowError, "unsupported arrow type: {}",
      data_type->name());
}
