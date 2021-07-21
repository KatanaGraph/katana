#ifndef KATANA_LIBSUPPORT_KATANA_ARROWVISITOR_H_
#define KATANA_LIBSUPPORT_KATANA_ARROWVISITOR_H_

#include <arrow/api.h>
#include <arrow/vendored/datetime/date.h>

#include "katana/ErrorCode.h"
#include "katana/Logging.h"
#include "katana/Result.h"

namespace katana {

//////////////////////////////////////////////////////////
// Arrow Visitor
/// This enables Arrow visitors of the form:
/// class Visitor {
///   using ReturnType = void; // configurable
///
///   template <typename ArrowType, typename ArgumentType>
///   katana::Result<ReturnType> Call(ArgumentType arg);
/// };
///
/// Visitor visitor;
/// katana::VisitArrow(array, visitor);

// These are fixed in arrow 3.0, but had a typo in 2.0
// Use in place of:
// - arrow::is_list_type
// - arrow::enable_if_list_type
// TODO(daniel) delete after upgrade
template <typename T>
using is_list_type_patched = std::integral_constant<
    bool, std::is_same<T, arrow::ListType>::value ||
              std::is_same<T, arrow::LargeListType>::value ||
              std::is_same<T, arrow::FixedSizeListType>::value>;
template <typename T, typename R = void>
using enable_if_list_type_patched =
    arrow::enable_if_t<is_list_type_patched<T>::value, R>;

namespace internal {

// A VisitorBaseType specifies the type of argument that
// VisitArrowInternal will accept, how to determine its
// datatype, and how to specialize it to its particular type.
// Currently the supported types are:
// - const arrow::Array&
// - const arrow::Scalar&
// - arrow::ArrayBuilder*

// Arrays are immutable, pass by const reference
struct ArrayVisitorBaseType {
  using ParamBase = const arrow::Array&;

  template <typename ArrowType>
  using ParamType = const typename arrow::TypeTraits<ArrowType>::ArrayType&;

  template <arrow::Type::type EnumType>
  static const auto& Cast(const arrow::Array& array) {
    using ArrowType = typename arrow::TypeIdTraits<EnumType>::Type;
    return static_cast<ParamType<ArrowType>>(array);
  }

  static std::shared_ptr<arrow::DataType> Type(const arrow::Array& array) {
    return array.type();
  }
};

// Scalars are immutable, pass by const reference
struct ScalarVisitorBaseType {
  using ParamBase = const arrow::Scalar&;

  template <typename ArrowType>
  using ParamType = const typename arrow::TypeTraits<ArrowType>::ScalarType&;

  template <arrow::Type::type EnumType>
  static const auto& Cast(const arrow::Scalar& scalar) {
    using ArrowType = typename arrow::TypeIdTraits<EnumType>::Type;
    return static_cast<ParamType<ArrowType>>(scalar);
  }

  static std::shared_ptr<arrow::DataType> Type(const arrow::Scalar& scalar) {
    return scalar.type;
  }
};

// Builders are mutable, pass by pointer
struct BuilderVisitorBaseType {
  using ParamBase = arrow::ArrayBuilder*;

  template <typename ArrowType>
  using ParamType = typename arrow::TypeTraits<ArrowType>::BuilderType*;

  template <arrow::Type::type EnumType>
  static auto Cast(arrow::ArrayBuilder* builder) {
    using ArrowType = typename arrow::TypeIdTraits<EnumType>::Type;
    return static_cast<ParamType<ArrowType>>(builder);
  }

  static std::shared_ptr<arrow::DataType> Type(arrow::ArrayBuilder* builder) {
    return builder->type();
  }
};

/// This function attempts to keep Arrow type-tests to a minimum by
/// encapsulating the desired behavior. Its behavior is to:
/// 1) Identify the Arrow type of the parameter
/// 2) Specialize the parameter for use in a template environment
/// 3) Call the templated visitor with the specialized parameter
///
/// In an ideal world, this is the *only* switch over Arrow types
/// in the whole repo. Everything should either handle arrow data
/// in a type-agnostic way, or use this interface to reach a
/// type-aware template environment. It probably recreates
/// functionality that exists elsewhere, notably in arrow::compute
///
/// A typical use case would involve creating a Visitor class
/// and implementing methods to handle various types, eg:
/// class Visitor {
///   using ReturnType = void; // configurable
///
///   template <typename ArrowType, typename ArgumentType>
///   katana::Result<ReturnType> Call(ArgumentType arg);
/// };
/// Arrow's type_traits.h offers tools to use SFINAE to differentiate
/// types and write appropriate Call functions for each type


// The Wrapper struct is used to keep track of the ArrowType for each argument
// as we recursively cast each argument. The ArrowTypes are then used to call
// the proper Call method of the Visitor class.
template <class ...ArrowTypes>
struct Wrapper {

  // This function calls the visitor's templated Call method with the arguments
  // appropriately casted
  template <class VisitorType, class ...Processed, size_t ...I>
  static katana::Result<typename std::decay_t<VisitorType>::ReturnType>
  VisitArrowInternalCall(
      VisitorType&& visitor, std::tuple<Processed...>&& processed,
      std::index_sequence<I...>) {
    return visitor.template Call<ArrowTypes...>(std::get<I>(processed)...);
  }


  // This function is the base-case for the VisitArrowInternal function,
  // where all the arguments except one have been appropriately casted.
  // It casts the final argument, determines the ArrowType for the last
  // argument, and passes a full tuple of casted arguments
  // to the VisitArrowInternalCall function. The ArrowTypes are passed
  // through the Wrapper struct.
  template <class VisitorBaseType, class VisitorType, class ...Processed>
  static katana::Result<typename std::decay_t<VisitorType>::ReturnType>
  VisitArrowInternal(
      VisitorType&& visitor, std::tuple<Processed...>&& processed,
      typename VisitorBaseType::ParamBase param) {
    switch (VisitorBaseType::Type(param)->id()) {
#define TYPE_CASE(EnumType)                                                      \
    case arrow::Type::EnumType: {                                                \
      using ArrowType =                                                          \
          typename arrow::TypeIdTraits<arrow::Type::EnumType>::Type;             \
      auto new_processed = std::tuple_cat(processed,                             \
          std::make_tuple(VisitorBaseType::template                              \
            Cast<arrow::Type::EnumType>(param)));                                \
      return Wrapper<ArrowTypes..., ArrowType>::template VisitArrowInternalCall( \
          std::forward<VisitorType>(visitor), std::move(new_processed),          \
          std::make_index_sequence<sizeof...(Processed)+1>{});                   \
    }
      TYPE_CASE(INT8)
      TYPE_CASE(UINT8)
      TYPE_CASE(INT16)
      TYPE_CASE(UINT16)
      TYPE_CASE(INT32)
      TYPE_CASE(UINT32)
      TYPE_CASE(INT64)
      TYPE_CASE(UINT64)
      TYPE_CASE(FLOAT)
      TYPE_CASE(DOUBLE)
      TYPE_CASE(BOOL)
      TYPE_CASE(DATE32)     // since UNIX epoch in days
      TYPE_CASE(DATE64)     // since UNIX epoch in millis
      TYPE_CASE(TIME32)     // since midnight in seconds or millis
      TYPE_CASE(TIME64)     // since midnight in micros or nanos
      TYPE_CASE(TIMESTAMP)  // since UNIX epoch in seconds or smaller
      TYPE_CASE(STRING)     // TODO(daniel) DEPRECATED
      TYPE_CASE(LARGE_STRING)
      TYPE_CASE(STRUCT)
      TYPE_CASE(LIST)  // TODO(daniel) DEPRECATED
      TYPE_CASE(LARGE_LIST)
      TYPE_CASE(NA)
#undef TYPE_CASE
    default:
      return KATANA_ERROR(
          katana::ErrorCode::ArrowError,
          "unsupported Arrow type encountered ({})",
          VisitorBaseType::Type(param)->ToString());
    }
  }

  // This function is the inductive case for the VisitArrowInternal function,
  // where more than one argument remains to be casted. It casts a single
  // argument, appends the casted argument to the tuple of casted arguments,
  // then calls the next iteration of VisitArrowInternal. The correct ArrowTypes
  // are passed through the Wrapper struct.
  template <class VisitorBaseType, class VisitorType, class ...Processed, class ...Unprocessed>
  static katana::Result<typename std::decay_t<VisitorType>::ReturnType>
  VisitArrowInternal(
      VisitorType&& visitor, std::tuple<Processed...> processed,
      typename VisitorBaseType::ParamBase param, Unprocessed... unprocessed) {
    switch (VisitorBaseType::Type(param)->id()) {
#define TYPE_CASE(EnumType)                                                      \
    case arrow::Type::EnumType: {                                                \
      using ArrowType =                                                          \
          typename arrow::TypeIdTraits<arrow::Type::EnumType>::Type;             \
      auto new_processed = std::tuple_cat(processed,                             \
          std::make_tuple(VisitorBaseType::template                              \
            Cast<arrow::Type::EnumType>(param)));                                \
      return Wrapper<ArrowTypes..., ArrowType>::template                         \
                                  VisitArrowInternal<VisitorBaseType>(           \
            std::forward<VisitorType>(visitor), std::move(new_processed),        \
            unprocessed...);                                                     \
    }
      TYPE_CASE(INT8)
      TYPE_CASE(UINT8)
      TYPE_CASE(INT16)
      TYPE_CASE(UINT16)
      TYPE_CASE(INT32)
      TYPE_CASE(UINT32)
      TYPE_CASE(INT64)
      TYPE_CASE(UINT64)
      TYPE_CASE(FLOAT)
      TYPE_CASE(DOUBLE)
      TYPE_CASE(BOOL)
      TYPE_CASE(DATE32)     // since UNIX epoch in days
      TYPE_CASE(DATE64)     // since UNIX epoch in millis
      TYPE_CASE(TIME32)     // since midnight in seconds or millis
      TYPE_CASE(TIME64)     // since midnight in micros or nanos
      TYPE_CASE(TIMESTAMP)  // since UNIX epoch in seconds or smaller
      TYPE_CASE(STRING)     // TODO(daniel) DEPRECATED
      TYPE_CASE(LARGE_STRING)
      TYPE_CASE(STRUCT)
      TYPE_CASE(LIST)  // TODO(daniel) DEPRECATED
      TYPE_CASE(LARGE_LIST)
      TYPE_CASE(NA)
#undef TYPE_CASE
    default:
      return KATANA_ERROR(
          katana::ErrorCode::ArrowError,
          "unsupported Arrow type encountered ({})",
          VisitorBaseType::Type(param)->ToString());
    }
  }
};

}  // namespace internal

// TODO(Rob) make sure that all types are the same with an enable_if recursion
template <class VisitorType, class Arg0, class ...Args>
std::enable_if_t<
  std::is_same_v<Arg0, arrow::Array&>
  && std::conjunction_v<std::is_same<Arg0, Args>...>,
  katana::Result<typename std::decay_t<VisitorType>::ReturnType>>

VisitArrow(VisitorType&& visitor, Arg0 array, Args... args) {
  return internal::Wrapper<>::template VisitArrowInternal<internal::ArrayVisitorBaseType>(
      std::forward<VisitorType>(visitor), std::tuple<>{}, array);
}

template <class VisitorType>
auto
VisitArrow(const arrow::Array& array, VisitorType&& visitor) {
  return internal::Wrapper<>::template VisitArrowInternal<internal::ArrayVisitorBaseType>(
      std::forward<VisitorType>(visitor), std::tuple<>{}, array);
}

template <class VisitorType>
auto
VisitArrow(const std::shared_ptr<arrow::Array>& array, VisitorType&& visitor) {
  KATANA_LOG_DEBUG_ASSERT(array);
  const arrow::Array& ref = *(array.get());
  return VisitArrow(ref, std::forward<VisitorType>(visitor));
}

template <class VisitorType, class Arg0, class ...Args>
std::enable_if_t<
  std::is_same_v<Arg0, arrow::Scalar&>
  && std::conjunction_v<std::is_same<Arg0, Args>...>, 
  katana::Result<typename std::decay_t<VisitorType>::ReturnType>>
VisitArrow(VisitorType&& visitor, Arg0 scalar, Args... args) {
  return internal::Wrapper<>::template VisitArrowInternal<internal::ArrayVisitorBaseType>(
      std::forward<VisitorType>(visitor), std::tuple<>{}, scalar);
}

template <class VisitorType>
auto
VisitArrow(const arrow::Scalar& scalar, VisitorType&& visitor) {
  return internal::Wrapper<>::template VisitArrowInternal<internal::ScalarVisitorBaseType>(
      std::forward<VisitorType>(visitor), std::tuple<>{}, scalar);
}

template <class VisitorType, class Arg0, class ...Args>
std::enable_if_t<
  std::is_same_v<Arg0, arrow::ArrayBuilder*>
  && std::conjunction_v<std::is_same<Arg0, Args>...>, 
  katana::Result<typename std::decay_t<VisitorType>::ReturnType>>
VisitArrow(VisitorType&& visitor, Arg0 builder, Args... args) {
  return internal::Wrapper<>::template VisitArrowInternal<internal::ArrayVisitorBaseType>(
      std::forward<VisitorType>(visitor), std::tuple<>{}, builder);
}

template <class VisitorType>
auto
VisitArrow(
    const std::shared_ptr<arrow::Scalar>& scalar, VisitorType&& visitor) {
  KATANA_LOG_DEBUG_ASSERT(scalar);
  const arrow::Scalar& ref = *(scalar.get());
  return VisitArrow(ref, std::forward<VisitorType>(visitor));
}

template <class VisitorType>
auto
VisitArrow(arrow::ArrayBuilder* builder, VisitorType&& visitor) {
  KATANA_LOG_DEBUG_ASSERT(builder);
  return internal::Wrapper<>::template VisitArrowInternal<internal::BuilderVisitorBaseType>(
      std::forward<VisitorType>(visitor), std::tuple<>{}, builder);
}

template <class VisitorType>
auto
VisitArrow(
    const std::unique_ptr<arrow::ArrayBuilder>& builder,
    VisitorType&& visitor) {
  return VisitArrow(builder.get(), std::forward<VisitorType>(visitor));
}

class AppendScalarToBuilder {
public:
  using ReturnType = void;
  using ResultType = Result<ReturnType>;

  AppendScalarToBuilder(arrow::ArrayBuilder* builder) : builder_(builder) {
    KATANA_LOG_DEBUG_ASSERT(builder_);
  }

  Result<void> AppendNull() {
    if (auto st = builder_->AppendNull(); !st.ok()) {
      return KATANA_ERROR(
          katana::ErrorCode::ArrowError, "failed to allocate table: {}", st);
    }
    return ResultSuccess();
  }

  template <typename ArrowType, typename ScalarType>
  arrow::enable_if_null<ArrowType, ResultType> Call(const ScalarType&) {
    return AppendNull();
  }

  template <typename ArrowType, typename ScalarType>
  arrow::enable_if_t<
      arrow::is_number_type<ArrowType>::value ||
          arrow::is_boolean_type<ArrowType>::value ||
          arrow::is_temporal_type<ArrowType>::value,
      ResultType>
  Call(const ScalarType& scalar) {
    using BuilderType = typename arrow::TypeTraits<ArrowType>::BuilderType;
    if (!scalar.is_valid) {
      return AppendNull();
    }
    auto builder = dynamic_cast<BuilderType*>(builder_);
    KATANA_LOG_DEBUG_ASSERT(builder);
    if (auto st = builder->Append(scalar.value); !st.ok()) {
      return KATANA_ERROR(
          katana::ErrorCode::ArrowError, "failed to allocate table: {}", st);
    }
    return ResultSuccess();
  }

  template <typename ArrowType, typename ScalarType>
  arrow::enable_if_string_like<ArrowType, ResultType> Call(
      const ScalarType& scalar) {
    using BuilderType = typename arrow::TypeTraits<ArrowType>::BuilderType;
    if (!scalar.is_valid) {
      return AppendNull();
    }
    auto builder = dynamic_cast<BuilderType*>(builder_);
    KATANA_LOG_DEBUG_ASSERT(builder);
    if (auto st = builder->Append(scalar.value->ToString()); !st.ok()) {
      return KATANA_ERROR(
          katana::ErrorCode::ArrowError, "failed to allocate table: {}", st);
    }
    return ResultSuccess();
  }

  template <typename ArrowType, typename ScalarType>
  enable_if_list_type_patched<ArrowType, ResultType> Call(
      const ScalarType& scalar) {
    if (!scalar.is_valid) {
      return AppendNull();
    }

    using BuilderType = typename arrow::TypeTraits<ArrowType>::BuilderType;
    auto builder = dynamic_cast<BuilderType*>(builder_);
    KATANA_LOG_DEBUG_ASSERT(builder);
    if (auto st = builder->Append(); !st.ok()) {
      return KATANA_ERROR(
          katana::ErrorCode::ArrowError, "failed to allocate table: {}", st);
    }
    AppendScalarToBuilder visitor(builder->value_builder());
    for (int64_t i = 0, n = scalar.value->length(); i < n; ++i) {
      auto scalar_res = scalar.value->GetScalar(i);
      if (!scalar_res.ok()) {
        return KATANA_ERROR(
            katana::ErrorCode::ArrowError, "failed to get scalar");
      }
      if (auto res = VisitArrow(scalar_res.ValueOrDie(), visitor); !res) {
        return res.error();
      }
    }

    return ResultSuccess();
  }

  template <typename ArrowType, typename ScalarType>
  arrow::enable_if_struct<ArrowType, ResultType> Call(
      const ScalarType& scalar) {
    if (!scalar.is_valid) {
      return AppendNull();
    }

    auto* builder = dynamic_cast<arrow::StructBuilder*>(builder_);
    KATANA_LOG_DEBUG_ASSERT(builder);
    if (auto st = builder->Append(); !st.ok()) {
      return KATANA_ERROR(
          katana::ErrorCode::ArrowError, "failed to allocate table: {}", st);
    }
    for (int f = 0, n = scalar.value.size(); f < n; ++f) {
      AppendScalarToBuilder visitor(builder->field_builder(f));
      if (auto res = VisitArrow(scalar.value.at(f), visitor); !res) {
        return res.error();
      }
    }

    return ResultSuccess();
  }

private:
  arrow::ArrayBuilder* builder_;
};

////////////////////////////////////////////
// Visitor-based utility
/// Take a vector of scalars of type data_type and return an Array
/// scalars vector can contain nullptr entries
KATANA_EXPORT Result<std::shared_ptr<arrow::Array>> ArrayFromScalars(
    const std::vector<std::shared_ptr<arrow::Scalar>>& scalars,
    const std::shared_ptr<arrow::DataType>& type);

}  // namespace katana

#endif
