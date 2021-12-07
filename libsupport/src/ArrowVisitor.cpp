#include "katana/ArrowVisitor.h"

#include <arrow/array/builder_base.h>
#include <arrow/type_traits.h>

#include "katana/Logging.h"

namespace {

class AppendScalarToBuilderVisitor : public katana::ArrowVisitor {
public:
  using ResultType = katana::Result<void>;

  using AcceptTypes =
      std::tuple<katana::AcceptAllArrowTypes, katana::AcceptAllArrowTypes>;

  template <typename ScalarArrowType>
  static constexpr bool MatchNull = arrow::is_null_type<ScalarArrowType>::value;

  template <typename ScalarArrowType, typename BuilderArrowType>
  static constexpr bool MatchPrimitive =
      (arrow::is_number_type<ScalarArrowType>::value &&
       arrow::is_number_type<BuilderArrowType>::value) ||
      (arrow::is_boolean_type<ScalarArrowType>::value &&
       arrow::is_boolean_type<BuilderArrowType>::value) ||
      (arrow::is_temporal_type<ScalarArrowType>::value &&
       std::is_same<ScalarArrowType, BuilderArrowType>::value);

  template <typename ScalarArrowType, typename BuilderArrowType>
  static constexpr bool MatchString =
      katana::is_string_like_type_patched<ScalarArrowType>::value&&
          katana::is_string_like_type_patched<BuilderArrowType>::value;

  template <typename ScalarArrowType, typename BuilderArrowType>
  static constexpr bool MatchList =
      arrow::is_list_like_type<ScalarArrowType>::value&&
          arrow::is_list_like_type<BuilderArrowType>::value;

  template <typename ScalarArrowType, typename BuilderArrowType>
  static constexpr bool MatchStruct =
      arrow::is_struct_type<ScalarArrowType>::value&&
          arrow::is_struct_type<BuilderArrowType>::value;

  template <typename ScalarArrowType, typename BuilderArrowType>
  static constexpr bool MatchFailed =
      !(MatchNull<ScalarArrowType> ||
        MatchPrimitive<ScalarArrowType, BuilderArrowType> ||
        MatchString<ScalarArrowType, BuilderArrowType> ||
        MatchList<ScalarArrowType, BuilderArrowType> ||
        MatchStruct<ScalarArrowType, BuilderArrowType>);

  template <
      typename ScalarArrowType, typename BuilderArrowType, typename ScalarType,
      typename BuilderType>
  std::enable_if_t<MatchNull<ScalarArrowType>, ResultType> Call(
      const ScalarType&, BuilderType* builder) {
    KATANA_CHECKED(builder->AppendNull());
    return katana::ResultSuccess();
  }

  template <
      typename ScalarArrowType, typename BuilderArrowType, typename ScalarType,
      typename BuilderType>
  std::enable_if_t<
      MatchPrimitive<ScalarArrowType, BuilderArrowType>, ResultType>
  Call(const ScalarType& scalar, BuilderType* builder) {
    if (!scalar.is_valid) {
      KATANA_CHECKED(builder->AppendNull());
      return katana::ResultSuccess();
    }
    KATANA_CHECKED(builder->Append(scalar.value));
    return katana::ResultSuccess();
  }

  template <
      typename ScalarArrowType, typename BuilderArrowType, typename ScalarType,
      typename BuilderType>
  std::enable_if_t<MatchString<ScalarArrowType, BuilderArrowType>, ResultType>
  Call(const ScalarType& scalar, BuilderType* builder) {
    if (!scalar.is_valid) {
      KATANA_CHECKED(builder->AppendNull());
      return katana::ResultSuccess();
    }
    KATANA_CHECKED(builder->Append((arrow::util::string_view)(*scalar.value)));
    return katana::ResultSuccess();
  }

  template <
      typename ScalarArrowType, typename BuilderArrowType, typename ScalarType,
      typename BuilderType>
  std::enable_if_t<MatchList<ScalarArrowType, BuilderArrowType>, ResultType>
  Call(const ScalarType& scalar, BuilderType* builder) {
    if (!scalar.is_valid) {
      KATANA_CHECKED(builder->AppendNull());
      return katana::ResultSuccess();
    }
    KATANA_CHECKED(builder->Append());
    for (int64_t i = 0, n = scalar.value->length(); i < n; ++i) {
      std::shared_ptr<arrow::Scalar> elem =
          KATANA_CHECKED(scalar.value->GetScalar(i));
      KATANA_CHECKED(VisitArrow(*this, *elem, builder->value_builder()));
    }

    return katana::ResultSuccess();
  }

  template <
      typename ScalarArrowType, typename BuilderArrowType, typename ScalarType,
      typename BuilderType>
  std::enable_if_t<MatchStruct<ScalarArrowType, BuilderArrowType>, ResultType>
  Call(const ScalarType& scalar, BuilderType* builder) {
    if (!scalar.is_valid) {
      KATANA_CHECKED(builder->AppendNull());
      return katana::ResultSuccess();
    }

    KATANA_CHECKED(builder->Append());
    for (int f = 0, n = scalar.value.size(); f < n; ++f) {
      AppendScalarToBuilderVisitor visitor;
      KATANA_CHECKED(katana::VisitArrow(
          visitor, *scalar.value.at(f), builder->field_builder(f)));
    }

    return katana::ResultSuccess();
  }

  template <
      typename ScalarArrowType, typename BuilderArrowType, typename ScalarType,
      typename BuilderType>
  std::enable_if_t<MatchFailed<ScalarArrowType, BuilderArrowType>, ResultType>
  Call(const ScalarType& scalar, BuilderType* builder) {
    return AcceptFailed(scalar, builder);
  }

  ResultType AcceptFailed(
      const arrow::Scalar& scalar, arrow::ArrayBuilder* builder) {
    return KATANA_ERROR(
        katana::ErrorCode::ArrowError, "no matching type {}, {}",
        scalar.type->name(), builder->type()->name());
  }
};

struct ToArrayVisitor : public katana::ArrowVisitor {
  // Internal data and constructor
  const std::vector<std::shared_ptr<arrow::Scalar>>& scalars;
  ToArrayVisitor(const std::vector<std::shared_ptr<arrow::Scalar>>& input)
      : scalars(input) {}

  using ResultType = katana::Result<std::shared_ptr<arrow::Array>>;

  using AcceptTypes = std::tuple<katana::tuple_cat_t<
      katana::AcceptAllArrowTypes, std::tuple<arrow::FixedSizeBinaryType>>>;

  template <typename ArrowType, typename BuilderType>
  arrow::enable_if_null<ArrowType, ResultType> Call(BuilderType* builder) {
    KATANA_CHECKED(builder->AppendNulls(scalars.size()));
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
        KATANA_CHECKED(
            builder->Append((arrow::util::string_view)(*typed_scalar->value)));
      } else {
        KATANA_CHECKED(builder->AppendNull());
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
    AppendScalarToBuilderVisitor visitor;
    for (const auto& scalar : scalars) {
      if (scalar != nullptr && scalar->is_valid) {
        const ScalarType* typed_scalar = static_cast<ScalarType*>(scalar.get());
        KATANA_CHECKED(
            (visitor.Call<ArrowType, ArrowType>(*typed_scalar, builder)));
      } else {
        KATANA_CHECKED(builder->AppendNull());
      }
    }
    return KATANA_CHECKED(builder->Finish());
  }

  template <typename ArrowType, typename BuilderType>
  arrow::enable_if_fixed_size_binary<ArrowType, ResultType> Call(
      BuilderType* builder) {
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

  ResultType AcceptFailed(const arrow::ArrayBuilder* builder) {
    return KATANA_ERROR(
        katana::ErrorCode::ArrowError, "no matching type {}",
        builder->type()->name());
  }
};

}  // namespace

katana::Result<void>
katana::AppendToBuilder(
    const arrow::Scalar& scalar, arrow::ArrayBuilder* builder) {
  AppendScalarToBuilderVisitor visitor;
  return katana::VisitArrow(visitor, scalar, builder);
}

katana::Result<std::shared_ptr<arrow::Array>>
katana::ArrayFromScalars(
    const arrow::ScalarVector& scalars,
    const std::shared_ptr<arrow::DataType>& type) {
  std::unique_ptr<arrow::ArrayBuilder> builder;
  KATANA_CHECKED(
      arrow::MakeBuilder(arrow::default_memory_pool(), type, &builder));
  for (size_t i = 0, n = scalars.size(); i < n; ++i) {
    if (scalars[i] && scalars[i]->is_valid) {
      KATANA_CHECKED(builder->AppendScalar(*scalars[i]));
    } else {
      KATANA_CHECKED(builder->AppendNull());
    }
  }
  return KATANA_CHECKED(builder->Finish());
}
