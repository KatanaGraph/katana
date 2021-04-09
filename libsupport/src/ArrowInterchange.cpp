#include "katana/ArrowInterchange.h"

#include "katana/Random.h"

namespace {

katana::Result<std::shared_ptr<arrow::ChunkedArray>>
IndexedTake(
    const std::shared_ptr<arrow::ChunkedArray>& original,
    std::shared_ptr<arrow::Array> indices) {
  // Use Take to select those indices
  arrow::Result<arrow::Datum> take_result =
      arrow::compute::Take(original, indices);
  if (!take_result.ok()) {
    return KATANA_ERROR(
        katana::ErrorCode::ArrowError,
        "arrow builder reserve failed type: {} reason: {}",
        original->type()->name(), take_result.status().CodeAsString());
  }
  arrow::Datum take = std::move(take_result.ValueOrDie());
  std::shared_ptr<arrow::ChunkedArray> chunked = take.chunked_array();
  KATANA_LOG_ASSERT(chunked->num_chunks() == 1);
  return chunked;
}

katana::Result<std::shared_ptr<arrow::Array>>
Indices(const std::shared_ptr<arrow::ChunkedArray>& original) {
  int64_t length = original->length();
  // Build indices array, reusable across properties
  arrow::CTypeTraits<int64_t>::BuilderType builder;
  auto res = builder.Reserve(length);
  if (!res.ok()) {
    return KATANA_ERROR(
        katana::ErrorCode::ArrowError,
        "arrow builder reserve failed type: {} reason: {}",
        original->type()->name(), res.CodeAsString());
  }
  for (int64_t i = 0; i < length; ++i) {
    builder.UnsafeAppend(i);
  }
  std::shared_ptr<arrow::Array> indices;
  res = builder.Finish(&indices);
  if (!res.ok()) {
    return KATANA_ERROR(
        katana::ErrorCode::ArrowError,
        "arrow shuffle builder failed type: {} reason: {}",
        original->type()->name(), res.CodeAsString());
  }
  return indices;
}

template <typename ArrowArrayType>
void
DoPrintFirstNonEqualElements(
    fmt::memory_buffer* buf, const std::shared_ptr<arrow::ChunkedArray>& a0,
    const std::shared_ptr<arrow::ChunkedArray>& a1, int32_t num_elts_to_print) {
  auto a0_arr = katana::Unchunk(a0);
  if (!a0_arr) {
    KATANA_LOG_DEBUG("failed to unchunk first array: {}", a0_arr.error());
    return;
  }
  auto cast_res = katana::ViewCast<ArrowArrayType>(a0_arr.value());
  if (!cast_res) {
    KATANA_LOG_DEBUG("failed to cast array: {}", cast_res.error());
    return;
  }
  auto b0 = cast_res.value();
  auto a1_arr = katana::Unchunk(a1);
  if (!a1_arr) {
    KATANA_LOG_DEBUG("failed to unchunk second array: {}", a1_arr.error());
    return;
  }
  cast_res = katana::ViewCast<ArrowArrayType>(a1_arr.value());
  if (!cast_res) {
    KATANA_LOG_DEBUG("failed to cast array: {}", cast_res.error());
    return;
  }
  auto b1 = cast_res.value();

  for (int64_t i = 0; i < b0->length(); ++i) {
    if (i >= b1->length()) {
      return;
    }
    if (b0->Value(i) != b1->Value(i)) {
      int64_t end = std::min(i + num_elts_to_print, b0->length());
      end = std::min(end, b1->length());
      for (int64_t j = i; j < end; j += 3) {
        switch (end - j) {
        case 1: {
          fmt::format_to(
              *buf, "{:7}: {:8} {:8}\n", j, b0->Value(j), b1->Value(j));
          break;
        }
        case 2: {
          fmt::format_to(
              *buf, "{:7}: {:8} {:8} {:7}: {:8} {:8}\n", j, b0->Value(j),
              b1->Value(j), j + 1, b0->Value(j + 1), b1->Value(j + 1));
          break;
        }
        default: {
          fmt::format_to(
              *buf, "{:7}: {:8} {:8} {:7}: {:8} {:8} {:7}: {:8} {:8}\n", j,
              b0->Value(j), b1->Value(j), j + 1, b0->Value(j + 1),
              b1->Value(j + 1), j + 2, b0->Value(j + 2), b1->Value(j + 2));
          break;
        }
        }
      }
      return;
    }
  }
}

}  // anonymous namespace

katana::Result<std::shared_ptr<arrow::ChunkedArray>>
katana::UpdateChunkedArray(
    const std::shared_ptr<arrow::ChunkedArray>& chunka,
    const std::shared_ptr<arrow::Scalar>& scalar, int64_t position) {
  auto before = chunka->Slice(0, position);
  std::shared_ptr<arrow::ChunkedArray> after;
  if (position == chunka->length()) {
    after = NullChunkedArray(chunka->type(), 0);
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
        katana::ErrorCode::ArrowError, "adding scalar {} at position {} : {}",
        scalar->ToString(), position, maybe_array.status().CodeAsString());
  }
  chunks.push_back(maybe_array.ValueOrDie());
  for (int32_t i = 0; i < after->num_chunks(); ++i) {
    chunks.push_back(after->chunk(i));
  }
  return std::make_shared<arrow::ChunkedArray>(chunks);
}

katana::Result<std::shared_ptr<arrow::Array>>
katana::Unchunk(const std::shared_ptr<arrow::ChunkedArray>& original) {
  auto maybe_indices = Indices(original);
  if (!maybe_indices) {
    return maybe_indices.error();
  }
  auto maybe_chunked = IndexedTake(original, maybe_indices.value());
  if (!maybe_chunked) {
    return maybe_chunked.error();
  }
  return maybe_chunked.value()->chunk(0);
}

katana::Result<std::shared_ptr<arrow::ChunkedArray>>
katana::Shuffle(const std::shared_ptr<arrow::ChunkedArray>& original) {
  int64_t length = original->length();
  // Build indices array, reusable across properties
  std::vector<uint64_t> indices_vec(length);
  // fills the vector from 0 to indices_vec.size()-1
  std::iota(indices_vec.begin(), indices_vec.end(), 0);
  std::shuffle(indices_vec.begin(), indices_vec.end(), katana::GetGenerator());
  arrow::CTypeTraits<int64_t>::BuilderType builder;
  auto res = builder.Reserve(length);
  if (!res.ok()) {
    return KATANA_ERROR(
        katana::ErrorCode::ArrowError,
        "arrow builder reserve failed type: {} reason: {}",
        original->type()->name(), res);
  }
  for (int64_t i = 0; i < length; ++i) {
    builder.UnsafeAppend(indices_vec[i]);
  }
  std::shared_ptr<arrow::Array> indices;
  res = builder.Finish(&indices);
  if (!res.ok()) {
    return KATANA_ERROR(
        katana::ErrorCode::ArrowError,
        "arrow shuffle builder failed type: {} reason: {}",
        original->type()->name(), res);
  }
  return IndexedTake(original, indices);
}

std::shared_ptr<arrow::ChunkedArray>
katana::NullChunkedArray(
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
    const std::vector<std::shared_ptr<arrow::Scalar>>& data) {
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
    if (scalar->is_valid) {
      // Is there a better way?
      builder->UnsafeAppend(
          std::static_pointer_cast<const ScalarType>(scalar)->value);
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

template <typename BuilderType, typename ScalarType>
katana::Result<std::shared_ptr<arrow::Array>>
StringLikeToArray(
    const std::shared_ptr<arrow::DataType>& data_type,
    const std::vector<std::shared_ptr<arrow::Scalar>>& data) {
  if (data.empty()) {
    return katana::ResultSuccess();
  }
  auto* pool = arrow::default_memory_pool();
  auto builder = std::make_shared<BuilderType>(data_type, pool);
  for (const auto& scalar : data) {
    if (scalar->is_valid) {
      // ->value->ToString() works, scalar->ToString() yields "..."
      if (auto res = builder->Append(
              std::static_pointer_cast<ScalarType>(scalar)->value->ToString());
          !res.ok()) {
        return KATANA_ERROR(
            katana::ErrorCode::ArrowError,
            "arrow builder failed append type: {} : {}", scalar->type->name(),
            res.CodeAsString());
      }
    } else {
      if (auto res = builder->AppendNull(); !res.ok()) {
        return KATANA_ERROR(
            katana::ErrorCode::ArrowError,
            "arrow builder failed append null: {}", scalar->type->name(),
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
    const std::vector<std::shared_ptr<arrow::Scalar>>& data) {
  std::shared_ptr<arrow::Array> array;
  switch (data_type->id()) {
  case arrow::Type::LARGE_STRING: {
    return StringLikeToArray<
        arrow::LargeStringBuilder, arrow::LargeStringScalar>(data_type, data);
    break;
  }
  case arrow::Type::STRING: {
    return StringLikeToArray<arrow::StringBuilder, arrow::StringScalar>(
        data_type, data);
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

void
katana::PrintFirstNonEqualElements(
    fmt::memory_buffer* buf, const std::shared_ptr<arrow::ChunkedArray>& a0,
    const std::shared_ptr<arrow::ChunkedArray>& a1, int32_t num_elts_to_print) {
  if (a0->type() != a1->type()) {
    fmt::format_to(
        *buf, "Arrays are different types {}/{}\n", a0->type()->name(),
        a1->type()->name());
    return;
  }
  auto data_type = a0->type();
  switch (data_type->id()) {
    // We would require different code to print strings and other variable length data
  case arrow::Type::INT64: {
    return DoPrintFirstNonEqualElements<arrow::Int64Array>(
        buf, a0, a1, num_elts_to_print);
    break;
  }
  case arrow::Type::UINT64: {
    return DoPrintFirstNonEqualElements<arrow::UInt64Array>(
        buf, a0, a1, num_elts_to_print);
    break;
  }
  case arrow::Type::INT32: {
    return DoPrintFirstNonEqualElements<arrow::Int32Array>(
        buf, a0, a1, num_elts_to_print);
    break;
  }
  case arrow::Type::UINT32: {
    return DoPrintFirstNonEqualElements<arrow::UInt32Array>(
        buf, a0, a1, num_elts_to_print);
    break;
  }
  case arrow::Type::INT16: {
    return DoPrintFirstNonEqualElements<arrow::Int16Array>(
        buf, a0, a1, num_elts_to_print);
    break;
  }
  case arrow::Type::UINT16: {
    return DoPrintFirstNonEqualElements<arrow::UInt16Array>(
        buf, a0, a1, num_elts_to_print);
    break;
  }
  case arrow::Type::INT8: {
    return DoPrintFirstNonEqualElements<arrow::Int8Array>(
        buf, a0, a1, num_elts_to_print);
    break;
  }
  case arrow::Type::UINT8: {
    return DoPrintFirstNonEqualElements<arrow::UInt8Array>(
        buf, a0, a1, num_elts_to_print);
    break;
  }
  case arrow::Type::DOUBLE: {
    return DoPrintFirstNonEqualElements<arrow::DoubleArray>(
        buf, a0, a1, num_elts_to_print);
    break;
  }
  case arrow::Type::FLOAT: {
    return DoPrintFirstNonEqualElements<arrow::FloatArray>(
        buf, a0, a1, num_elts_to_print);

    break;
  }
  case arrow::Type::BOOL: {
    return DoPrintFirstNonEqualElements<arrow::BooleanArray>(
        buf, a0, a1, num_elts_to_print);
    break;
  }
  default: {
    break;
  }
  }
  fmt::format_to(*buf, "Arrays of unsupported type {}\n", data_type->name());
}
