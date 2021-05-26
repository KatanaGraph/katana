#include "katana/ArrowVisitor.h"

#include <arrow/type_traits.h>

#include "katana/Logging.h"

struct ToArrayVisitor {
  // Internal data and constructor
  const std::vector<std::shared_ptr<arrow::Scalar>>& scalars;
  ToArrayVisitor(const std::vector<std::shared_ptr<arrow::Scalar>>& input)
      : scalars(input) {}

  using ReturnType = std::shared_ptr<arrow::Array>;
  using ResultType = katana::Result<ReturnType>;
  template <typename ArrowType, typename BuilderType>
  arrow::enable_if_null<ArrowType, ResultType> Call(BuilderType* builder) {
    std::shared_ptr<arrow::Array> array;
    auto res = builder->Finish(&array);
    if (!res.ok()) {
      return KATANA_ERROR(
          katana::ErrorCode::ArrowError, "arrow builder finish: {}", res);
    }
    return array;
  }
  template <typename ArrowType, typename BuilderType>
  arrow::enable_if_t<
      arrow::is_number_type<ArrowType>::value ||
          arrow::is_boolean_type<ArrowType>::value ||
          arrow::is_temporal_type<ArrowType>::value,
      ResultType>
  Call(BuilderType* builder) {
    using ScalarType = typename arrow::TypeTraits<ArrowType>::ScalarType;

    if (auto st = builder->Reserve(scalars.size()); !st.ok()) {
      return KATANA_ERROR(
          katana::ErrorCode::ArrowError,
          "arrow builder failed resize: {} reason: {}", scalars.size(), st);
    }
    for (const auto& scalar : scalars) {
      if (scalar->is_valid) {
        const ScalarType* typed_scalar = static_cast<ScalarType*>(scalar.get());
        builder->UnsafeAppend(typed_scalar->value);
      } else {
        builder->UnsafeAppendNull();
      }
    }
    std::shared_ptr<arrow::Array> array;
    auto res = builder->Finish(&array);
    if (!res.ok()) {
      return KATANA_ERROR(
          katana::ErrorCode::ArrowError, "arrow builder finish: {}", res);
    }
    return array;
  }
  template <typename ArrowType, typename BuilderType>
  arrow::enable_if_string_like<ArrowType, ResultType> Call(
      BuilderType* builder) {
    using ScalarType = typename arrow::TypeTraits<ArrowType>::ScalarType;
    // same as above, but use scalar.value->ToString()
    for (const auto& scalar : scalars) {
      if (scalar->is_valid) {
        // ->value->ToString() works, scalar->ToString() yields "..."
        const ScalarType* typed_scalar = static_cast<ScalarType*>(scalar.get());
        if (auto res = builder->Append(typed_scalar->value->ToString());
            !res.ok()) {
          return KATANA_ERROR(
              katana::ErrorCode::ArrowError, "arrow builder failed append: {}",
              res);
        }
      } else {
        if (auto res = builder->AppendNull(); !res.ok()) {
          return KATANA_ERROR(
              katana::ErrorCode::ArrowError,
              "arrow builder failed append null: {}", res);
        }
      }
    }
    std::shared_ptr<arrow::Array> array;
    auto res = builder->Finish(&array);
    if (!res.ok()) {
      return KATANA_ERROR(
          katana::ErrorCode::ArrowError, "arrow builder finish: {}", res);
    }
    return array;
  }
  template <typename ArrowType, typename BuilderType>
  katana::enable_if_list_type_patched<ArrowType, ResultType> Call(
      BuilderType*) {
    KATANA_LOG_FATAL("List visitors not yet supported");
  }
  template <typename ArrowType, typename BuilderType>
  arrow::enable_if_struct<ArrowType, ResultType> Call(BuilderType*) {
    KATANA_LOG_FATAL("Struct visitors not yet supported");
  }
};

katana::Result<std::shared_ptr<arrow::Array>>
katana::ArrayFromScalars(
    const std::vector<std::shared_ptr<arrow::Scalar>>& scalars,
    const std::shared_ptr<arrow::DataType>& type) {
  std::unique_ptr<arrow::ArrayBuilder> builder;
  if (auto st =
          arrow::MakeBuilder(arrow::default_memory_pool(), type, &builder);
      !st.ok()) {
    return KATANA_ERROR(
        katana::ErrorCode::ArrowError,
        "arrow builder failed type: {} length {} : {}", type->name(),
        scalars.size(), st);
  }
  ToArrayVisitor visitor(scalars);

  return katana::VisitArrow(builder, visitor);
}
