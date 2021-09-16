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
    return KATANA_CHECKED(builder->Finish());
    ;
  }

  template <typename ArrowType, typename BuilderType>
  arrow::enable_if_t<
      arrow::is_number_type<ArrowType>::value ||
          arrow::is_boolean_type<ArrowType>::value ||
          arrow::is_temporal_type<ArrowType>::value,
      ResultType>
  Call(BuilderType* builder) {
    using ScalarType = typename arrow::TypeTraits<ArrowType>::ScalarType;

    KATANA_CHECKED(builder->Reserve(scalars.size()));
    for (const auto& scalar : scalars) {
      if (scalar != nullptr && scalar->is_valid) {
        const ScalarType* typed_scalar = static_cast<ScalarType*>(scalar.get());
        builder->UnsafeAppend(typed_scalar->value);
      } else {
        builder->UnsafeAppendNull();
      }
    }
    return KATANA_CHECKED(builder->Finish());
  }

  template <typename ArrowType, typename BuilderType>
  arrow::enable_if_string_like<ArrowType, ResultType> Call(
      BuilderType* builder) {
    using ScalarType = typename arrow::TypeTraits<ArrowType>::ScalarType;
    // same as above, but with string_view and Append instead of UnsafeAppend
    for (const auto& scalar : scalars) {
      if (scalar != nullptr && scalar->is_valid) {
        // ->value->ToString() works, scalar->ToString() yields "..."
        const ScalarType* typed_scalar = static_cast<ScalarType*>(scalar.get());
        if (auto res = builder->Append(
                (arrow::util::string_view)(*typed_scalar->value));
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
    return KATANA_CHECKED(builder->Finish());
  }

  template <typename ArrowType, typename BuilderType>
  std::enable_if_t<
      arrow::is_list_type<ArrowType>::value ||
          arrow::is_struct_type<ArrowType>::value,
      ResultType>
  Call(BuilderType* builder) {
    using ScalarType = typename arrow::TypeTraits<ArrowType>::ScalarType;
    // use a visitor to traverse more complex types
    katana::AppendScalarToBuilder visitor(builder);
    for (const auto& scalar : scalars) {
      if (scalar != nullptr && scalar->is_valid) {
        const ScalarType* typed_scalar = static_cast<ScalarType*>(scalar.get());
        KATANA_CHECKED(visitor.Call<ArrowType>(*typed_scalar));
      } else {
        KATANA_CHECKED(builder->AppendNull());
      }
    }
    return KATANA_CHECKED(builder->Finish());
  }
};

katana::Result<std::shared_ptr<arrow::Array>>
katana::ArrayFromScalars(
    const std::vector<std::shared_ptr<arrow::Scalar>>& scalars,
    const std::shared_ptr<arrow::DataType>& type) {
  std::unique_ptr<arrow::ArrayBuilder> builder;
  KATANA_CHECKED(
      arrow::MakeBuilder(arrow::default_memory_pool(), type, &builder));
  ToArrayVisitor visitor(scalars);

  return katana::VisitArrow(builder, visitor);
}
