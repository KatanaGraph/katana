#include "katana/ArrowVisitor.h"

#include <arrow/array/builder_base.h>
#include <arrow/type_traits.h>

#include "katana/Logging.h"

// TODO(ddn): Move visitor to a function, callers should not need to see this
// definition directly
class AppendScalarToBuilder : public ArrowVisitor {
public:
  using ResultType = Result<void>;

  using AcceptTypes = std::tuple<AcceptAllArrowTypes>;

  template <
      typename ArrowTypeScalar, typename ArrowTypeBuilder, typename BuilderType>
  arrow::enable_if_null<ArrowTypeScalar, ResultType> Call(
      const arrow::NullScalar&, BuilderType* builder) {
    return KATANA_CHECKED(builder->AppendNull());
  }

  template <
      typename ArrowTypeScalar, typename ArrowTypeBuilder, typename ScalarType,
      typename BuilderType>
  std::enable_if_t<
      std::is_same<ArrowTypeScalar, ArrowTypeBuilder>::value &&
          (arrow::is_number_type<ArrowType>::value ||
           arrow::is_boolean_type<ArrowType>::value ||
           arrow::is_temporal_type<ArrowType>::value),
      ResultType>
  Call(const ScalarType& scalar, BuilderType* builder) {
    if (!scalar.is_valid) {
      return KATANA_CHECKED(builder->AppendNull());
    }
    KATANA_CHECKED(builder->Append(scalar.value));
    return ResultSuccess();
  }

  template <typename ArrowType, typename ScalarType>
  arrow::enable_if_string_like<ArrowType, ResultType> Call(
      const ScalarType& scalar) {
    if (!scalar.is_valid) {
      return KATANA_CHECKED(builder->AppendNull());
    }
    KATANA_CHECKED(builder->Append(static_cast<arrow::util::string_view>(*scalar.value)));
    return ResultSuccess();
  }

  template <typename ArrowType, typename ScalarType>
  arrow::enable_if_list_type<ArrowType, ResultType> Call(
      const ScalarType& scalar) {
    if (!scalar.is_valid) {
      return AppendNull();
    }

    using BuilderType = typename arrow::TypeTraits<ArrowType>::BuilderType;
    auto& builder = dynamic_cast<BuilderType&>(*builder_);
    if (auto st = builder.Append(); !st.ok()) {
      return KATANA_ERROR(
          katana::ErrorCode::ArrowError, "failed to allocate table: {}", st);
    }
    AppendScalarToBuilder visitor(builder.value_builder());
    for (int64_t i = 0, n = scalar.value->length(); i < n; ++i) {
      auto scalar_res = scalar.value->GetScalar(i);
      if (!scalar_res.ok()) {
        return KATANA_ERROR(
            katana::ErrorCode::ArrowError, "failed to get scalar");
      }
      KATANA_CHECKED(VisitArrow(visitor, *scalar_res.ValueOrDie()));
    }

    return ResultSuccess();
  }

  template <typename ArrowType, typename ScalarType>
  arrow::enable_if_struct<ArrowType, ResultType> Call(
      const ScalarType& scalar) {
    if (!scalar.is_valid) {
      return AppendNull();
    }

    auto& builder = dynamic_cast<arrow::StructBuilder&>(*builder_);
    if (auto st = builder.Append(); !st.ok()) {
      return KATANA_ERROR(
          katana::ErrorCode::ArrowError, "failed to allocate table: {}", st);
    }
    for (int f = 0, n = scalar.value.size(); f < n; ++f) {
      AppendScalarToBuilder visitor(builder.field_builder(f));
      KATANA_CHECKED(VisitArrow(visitor, *scalar.value.at(f)));
    }

    return ResultSuccess();
  }

  ResultType AcceptFailed(
      const arrow::Scalar& scalar, arrow::ArrayBuilder* builder) {
    return KATANA_ERROR(
        katana::ErrorCode::ArrowError, "no matching types {}, {}",
        scalar.type->name(), builder->type()->name());
  }
};

struct ToArrayVisitor : public katana::ArrowVisitor {
  // Internal data and constructor
  const std::vector<std::shared_ptr<arrow::Scalar>>& scalars;
  ToArrayVisitor(const std::vector<std::shared_ptr<arrow::Scalar>>& input)
      : scalars(input) {}

  using ResultType = katana::Result<std::shared_ptr<arrow::Array>>;

  using AcceptTypes = std::tuple<katana::AcceptAllArrowTypes>;

  template <typename ArrowType, typename BuilderType>
  arrow::enable_if_null<ArrowType, ResultType> Call(BuilderType* builder) {
    return KATANA_CHECKED(builder->Finish());
  }

  template <typename ArrowType, typename BuilderType>
  std::enable_if_t<
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

  ResultType AcceptFailed(const arrow::ArrayBuilder* builder) {
    return KATANA_ERROR(
        katana::ErrorCode::ArrowError, "no matching type {}",
        builder->type()->name());
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

  return katana::VisitArrow(visitor, builder.get());
}
