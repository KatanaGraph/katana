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
    KATANA_CHECKED_CONTEXT(
        builder->AppendScalar(scalar), "incompatible types: appending {} to {}",
        scalar.type->ToString(), builder->type()->ToString());
    return katana::ResultSuccess();
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
