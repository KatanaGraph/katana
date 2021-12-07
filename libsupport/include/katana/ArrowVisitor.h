#ifndef KATANA_LIBSUPPORT_KATANA_ARROWVISITOR_H_
#define KATANA_LIBSUPPORT_KATANA_ARROWVISITOR_H_

#include <type_traits>

#include <arrow/api.h>
#include <arrow/type.h>
#include <arrow/vendored/datetime/date.h>
#include <boost/preprocessor/repetition/repeat.hpp>

#include "katana/ErrorCode.h"
#include "katana/Logging.h"
#include "katana/Result.h"

namespace katana {

/// is_string_like_type_patched is true if the given type is string-like,
/// which for arrow means that it is a variable-sized type that is UTF8
/// encoded.
///
/// Unlike arrow::is_string_like_type, this version is total, i.e., has a value
/// for all types. arrow::is_string_like_type is only defined for types that
/// have the member is_utf8.
///
/// test/arrow.cpp tracks the necessity of having this workaround.
template <typename T>
struct is_string_like_type_patched {
  template <typename U>
  constexpr static arrow::enable_if_string_like<U, bool> test(void*) {
    return true;
  }

  template <typename>
  constexpr static bool test(...) {
    return false;
  }

  constexpr static bool value = test<T>(nullptr);
};

/// GetArrowTypeID returns the arrow type ID for a parameter. This is an
/// extension point for VisitArrow. Overload this and VisitArrowCast for custom
/// parameter types.
inline arrow::Type::type
GetArrowTypeID(const arrow::Scalar& scalar) {
  return scalar.type->id();
}

/// VisitArrowCast downcasts or specializes a general type to its more specific
/// type. The resulting type should be consistent with GetArrowTypeID. This is
/// an extension point for VisitArrow. Overload this and GetArrowTypeID to
/// support custom parameter types.
template <typename T>
constexpr decltype(auto)
VisitArrowCast(const arrow::Scalar& scalar) {
  // decltype(auto) is the complement of perfect forwarding,
  // std::forward<T>(item), for return values. With just auto, we will get the
  // decayed type, which will create a copy when casting to a reference type.
  using ResultType = const typename arrow::TypeTraits<T>::ScalarType&;
  return static_cast<ResultType>(scalar);
}

inline arrow::Type::type
GetArrowTypeID(const arrow::Array& array) {
  return array.type()->id();
}

template <typename T>
constexpr decltype(auto)
VisitArrowCast(const arrow::Array& array) {
  using ResultType = const typename arrow::TypeTraits<T>::ArrayType&;
  return static_cast<ResultType>(array);
}

inline arrow::Type::type
GetArrowTypeID(const arrow::ArrayBuilder* builder) {
  return builder->type()->id();
}

template <typename T>
constexpr auto
VisitArrowCast(arrow::ArrayBuilder* builder) {
  using ResultType = typename arrow::TypeTraits<T>::BuilderType*;
  return static_cast<ResultType>(builder);
}

inline arrow::Type::type
GetArrowTypeID(const arrow::DataType& type) {
  return type.id();
}

template <typename T>
constexpr decltype(auto)
VisitArrowCast(const arrow::DataType& type) {
  return static_cast<const T&>(type);
}

/// Concept for visitors for VisitArrow.
///
/// A visitor for VisitArrow should model the following behavior.
///
/// Users can optionally subclass ArrowVisitor to signal they intend to follow
/// the ArrowVisitor protocol but this is not required.
///
/// Example:
///
///     class MyVisitor {
///     public:
///       /// Return type of Call and AcceptFailed.
///       using ResultType = void;
///
///       /// AcceptTypes gives the types accepted by Call as a tuple of tuples of
///       /// arrow types. The ith tuple are the types accepted by the ith parameter to
///       /// Call.
///       ///
///       /// Some care should be taken for visitors that accept more than one
///       /// parameter as the number of template instantiations grows exponentially
///       /// with the number of arguments.
///       ///
///       /// Prefer placing the most constrained parameters first in AcceptTypes and
///       /// consider currying a visitor that takes k parameters into k visitors that
///       /// take 1 parameter.
///       using AcceptTypes = std::tuple<katana::AcceptNumericArrowTypes>;
///
///       /// Call is invoked by VisitArrow with the runtime arrow types lifted to
///       /// ArrowTypes (e.g., arrow::Int32Type) and the parameters downcasted or
///       /// specialized from their static parameter types (e.g., arrow::Scalar) to
///       /// their specific runtime types (e.g., arrow::Int32Scalar).
///       template <typename ArrowType, typename ArrayType>
///       ResultType Call(const ArrayType& array) {
///         // Due to AcceptTypes, VisitArrow will only invoke Call on numeric types.
///         // In all other cases, AcceptFailed will be used instead.
///       }
///
///       /// AcceptFailed is called by VisitArrow when there is no matching call. This
///       /// happens if the runtime type does not match one known by VisitArrow or if
///       /// the runtime type does not match AcceptTypes. AcceptFailed is invoked with
///       /// the original arguments to VisitArrow.
///       ResultType AcceptFailed(const arrow::Array&);
///     };
class ArrowVisitor {
  // Declarations are omitted to give clearer compiler error messages.
};

namespace internal {

/// ArrowDispatcher uses runtime arrow type information to downcast or
/// specialize general arguments like arrow::Scalar to their specific runtime
/// type like arrow::Int32Scalar and call a function on a visitor with the
/// those arguments along with their corresponding arrow types.
///
/// Effectively, given it converts
///
///     ArrowDispatcher::Call(visitor, arrow::Scalar, arrow::Array)
///
/// into
///
///    visitor.Call<arrow::Int32Type, arrow::Int64Type>(arrow::Int32Scalar, arrow::Int64Array)
///
/// See VisitArrow for details on how this is typically used.
struct ArrowDispatcher {
  template <typename V, typename Tuple, size_t... I>
  constexpr static bool TupleContains(std::index_sequence<I...>) {
    return (std::is_same_v<V, std::tuple_element_t<I, Tuple>> || ...);
  }

  // CouldAccept returns true if the ith ArrowType is contained in the ith
  // Visitor::AcceptTypes tuple.
  template <size_t I, typename Visitor>
  constexpr static bool CouldAccept() {
    return true;
  }

  template <
      size_t I, typename Visitor, typename ArrowType, typename... ArrowTypes>
  constexpr static bool CouldAccept() {
    using Current =
        std::tuple_element_t<I, std::tuple<ArrowType, ArrowTypes...>>;
    using CurrentAcceptTuple =
        std::tuple_element_t<I, typename Visitor::AcceptTypes>;
    constexpr size_t N = std::tuple_size_v<CurrentAcceptTuple>;
    return TupleContains<Current, CurrentAcceptTuple>(
        std::make_index_sequence<N>());
  }

  template <typename... ArrowTypes, typename Visitor, typename... Args>
  static typename std::decay_t<Visitor>::ResultType Call(
      Visitor&& visitor, Args&&... args) {
    constexpr auto index = sizeof...(ArrowTypes) - 1;
    if constexpr (!CouldAccept<index, std::decay_t<Visitor>, ArrowTypes...>()) {
      return visitor.AcceptFailed(std::forward<Args>(args)...);
    } else if constexpr (index + 1 == sizeof...(Args)) {
      return visitor.template Call<ArrowTypes...>(
          VisitArrowCast<ArrowTypes>(std::forward<Args>(args))...);
    } else {
      return Dispatch<ArrowTypes...>(
          std::forward<Visitor>(visitor), std::forward<Args>(args)...);
    }
  }

  template <typename... ArrowTypes, typename Visitor, typename... Args>
  static typename std::decay_t<Visitor>::ResultType Dispatch(
      Visitor&& visitor, Args&&... args) {
    constexpr auto index = sizeof...(ArrowTypes);
    auto type_id =
        GetArrowTypeID(std::get<index>(std::forward_as_tuple(args...)));

    // switch over the universe of Arrow types
    // filtering the universe is handled by CouldAccept
    switch (type_id) {
#define TYPE_CASE(EnumType)                                                    \
  case arrow::Type::EnumType: {                                                \
    using ArrowType =                                                          \
        typename arrow::TypeIdTraits<arrow::Type::EnumType>::Type;             \
    return Call<ArrowTypes..., ArrowType>(                                     \
        std::forward<Visitor>(visitor), std::forward<Args>(args)...);          \
  }
      TYPE_CASE(NA)
      TYPE_CASE(BOOL)
      TYPE_CASE(UINT8)
      TYPE_CASE(INT8)
      TYPE_CASE(UINT16)
      TYPE_CASE(INT16)
      TYPE_CASE(UINT32)
      TYPE_CASE(INT32)
      TYPE_CASE(UINT64)
      TYPE_CASE(INT64)
      TYPE_CASE(HALF_FLOAT)
      TYPE_CASE(FLOAT)
      TYPE_CASE(DOUBLE)
      TYPE_CASE(STRING)
      TYPE_CASE(BINARY)
      TYPE_CASE(FIXED_SIZE_BINARY)
      TYPE_CASE(DATE32)
      TYPE_CASE(DATE64)
      TYPE_CASE(TIMESTAMP)
      TYPE_CASE(TIME32)
      TYPE_CASE(TIME64)
      TYPE_CASE(INTERVAL_MONTHS)
      TYPE_CASE(INTERVAL_DAY_TIME)
      TYPE_CASE(DECIMAL128)
      TYPE_CASE(DECIMAL256)
      TYPE_CASE(LIST)
      TYPE_CASE(STRUCT)
      TYPE_CASE(SPARSE_UNION)
      TYPE_CASE(DENSE_UNION)
      TYPE_CASE(DICTIONARY)
      TYPE_CASE(MAP)
      TYPE_CASE(EXTENSION)
      TYPE_CASE(FIXED_SIZE_LIST)
      TYPE_CASE(DURATION)
      TYPE_CASE(LARGE_STRING)
      TYPE_CASE(LARGE_BINARY)
      TYPE_CASE(LARGE_LIST)
      TYPE_CASE(INTERVAL_MONTH_DAY_NANO)
#undef TYPE_CASE
    default:
      return visitor.AcceptFailed(std::forward<Args>(args)...);
    }

    // leaving this here to draw attention to the switch above upon changing arrow versions
    // arrow 6.0 has 38 types
    static_assert(arrow::Type::MAX_ID == 38);
  }
};

template <typename T>
constexpr bool
PointerOrReferenceType() {
  return std::is_pointer_v<T> || std::is_reference_v<T>;
}
}  // namespace internal

/// VisitArrow downcasts or specializes its arguments to their specific runtime
/// types and then invokes Visitor::Call.
///
/// For example,
///
///     VisitArrow(visitor, arrow::Scalar, arrow::Array)
///
/// may invoke
///
///     visitor.Call<arrow::Int32Type, arrow::Int64Type>(arrow::Int32Scalar, arrow::Int64Array)
///
/// depending on the runtime types of the arguments to VisitArrow.
///
/// A typical visitor looks like:
///
///     class Visitor : public ArrowVisitor {
///     public:
///       using ResultType = Result<int>;
///       using AcceptTypes = std::tuple<AcceptNumericArrowTypes>;
///
///       template <typename ArrowType, typename ScalarType>
///       ResultType
///       Call(const ScalarType& arg) {
///         // return something for numeric scalar types
///       }
///
///       ResultType
///       AcceptFailed(const arrow::Scalar& arg) {
///         // perhaps return an error when we receive an unexpected type
///       }
///     };
///
/// VisitArrow accepts arrow::Scalar, arrow::Array and arrow::ArrayBuilder*. To
/// extend the set of valid parameter types, see GetArrowTypeID and
/// VisitArrowCast.
template <typename Visitor, typename... Args>
auto
VisitArrow(Visitor&& visitor, Args&&... args) {
  static_assert(
      (... && internal::PointerOrReferenceType<Args>()),
      "cannot visit value types");
  return internal::ArrowDispatcher::Call(
      std::forward<Visitor>(visitor), std::forward<Args>(args)...);
}

using AcceptNumericArrowTypes = std::tuple<
    arrow::Int8Type, arrow::UInt8Type, arrow::Int16Type, arrow::UInt16Type,
    arrow::Int32Type, arrow::UInt32Type, arrow::Int64Type, arrow::UInt64Type,
    arrow::FloatType, arrow::DoubleType>;

using AcceptListArrowTypes = std::tuple<arrow::ListType, arrow::LargeListType>;

using AcceptStringArrowTypes =
    std::tuple<arrow::StringType, arrow::LargeStringType>;

using AcceptNullArrowTypes = std::tuple<arrow::NullType>;

using AcceptInstantArrowTypes = std::tuple<
    arrow::Date32Type, arrow::Date64Type, arrow::Time32Type, arrow::Time64Type,
    arrow::TimestampType>;

using AcceptAllArrowTypes = std::tuple<
    arrow::Int8Type, arrow::UInt8Type, arrow::Int16Type, arrow::UInt16Type,
    arrow::Int32Type, arrow::UInt32Type, arrow::Int64Type, arrow::UInt64Type,
    arrow::FloatType, arrow::DoubleType, arrow::FloatType, arrow::DoubleType,
    arrow::BooleanType, arrow::Date32Type, arrow::Date64Type, arrow::Time32Type,
    arrow::Time64Type, arrow::TimestampType, arrow::StringType,
    arrow::LargeStringType, arrow::StructType, arrow::ListType,
    arrow::LargeListType, arrow::NullType>;

template <typename... Args>
using tuple_cat_t = decltype(std::tuple_cat(std::declval<Args>()...));

/// Append a single scalar to a builder of compatible type
/// Use of arrow::Scalar is often inefficient, consider alternatives
KATANA_EXPORT Result<void> AppendToBuilder(
    const arrow::Scalar& scalar, arrow::ArrayBuilder* builder);

/// Take a vector of scalars of type data_type and return an Array
/// scalars vector can contain nullptr entries
KATANA_EXPORT Result<std::shared_ptr<arrow::Array>> ArrayFromScalars(
    const std::vector<std::shared_ptr<arrow::Scalar>>& scalars,
    const std::shared_ptr<arrow::DataType>& type);

}  // namespace katana

#endif
