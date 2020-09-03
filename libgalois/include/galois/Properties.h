#ifndef GALOIS_LIBGALOIS_GALOIS_PROPERTIES_H_
#define GALOIS_LIBGALOIS_GALOIS_PROPERTIES_H_

#include <string_view>
#include <cassert>
#include <utility>

#include <arrow/type_fwd.h>
#include <arrow/type_traits.h>
#include <arrow/array.h>

#include "galois/Traits.h"
#include "galois/Result.h"

namespace galois {

/// A property is a value associated with a node or edge of a graph. Properies
/// are stored in arrow::Arrays, and the arrow library collects multiple
/// properties (arrow: columns) in an arrow::Table.
///
/// For our purposes, a property is a way to identify a column in an
/// arrow::Table and its possible representation type. The following are
/// examples of properties:
///
/// - the rank label of a node; type: int32_t
/// - the height label of a node; type: int32_t
/// - the distance label of an edge; type: uint32_t
///
/// The same underlying data, an array of 32-bit values, can represent a number
/// of potential properties. It is up to users to impose a particular typed
/// view on top of their raw data. A PropertyFileGraph manages raw, untyped
/// data, and a PropertyGraph provides typed property views on top of a
/// PropertyFileGraph.
///
/// There are two ways of configuring a property for a PropertyGraph.
///
/// The most common way is to create a new type with two nested types:
/// ArrowType and ViewType:
///
///   struct Rank {
///     using ArrowType = arrow::Int32;
///     using ViewType = galois::PODPropertyView<int32_t>;
///   };
///
/// The other way would be to partially specialize PropertyArrowType,
/// PropertyViewType, PropertyArrowArrayType and PropertyReferenceType for your
/// specific property type.
///
/// Once configured, properties can be used as follows:
///
///   PropertyFileGraph raw_graph = PropertyFileGraph::Make(....);
///
///   using NodeData = std::tuple<Rank>;
///   using EdgeData = std::tuple<Distance>;
///   using Graph = PropertyGraph<NodeData, EdgeData>;
///
///   Graph graph = Graph::Make(....);
///
///   Graph::node_iterator node = ....;
///
///   int32_t& rank = graph.GetData<Rank>(node);
template <typename Prop>
struct PropertyTraits {
  using ArrowType = typename Prop::ArrowType;
  using ViewType  = typename Prop::ViewType;
};

template <typename Prop>
using PropertyArrowType = typename PropertyTraits<Prop>::ArrowType;

template <typename Prop>
using PropertyViewType = typename PropertyTraits<Prop>::ViewType;

template <typename Prop>
using PropertyArrowArrayType =
    typename arrow::TypeTraits<PropertyArrowType<Prop>>::ArrayType;

template <typename Prop>
using PropertyReferenceType = typename PropertyViewType<Prop>::reference;

namespace internal {

template <typename>
struct PropertyViewTuple;

template <>
struct PropertyViewTuple<void> {
  using type = std::tuple<>;
};

template <typename... Args>
struct PropertyViewTuple<std::tuple<Args...>> {
  using type = std::tuple<galois::PropertyViewType<Args>...>;
};

} // namespace internal

/// PropertyViewTuple applies PropertyViewType to a tuple of properties.
template <typename T>
using PropertyViewTuple = typename internal::PropertyViewTuple<T>::type;

/// TupleElements selects the tuple elements at the given indices
template <typename Tuple, size_t... indices>
using TupleElements = std::tuple<std::tuple_element_t<indices, Tuple>...>;

/// ConstructPropertyView applies a property view to an arrow::Array.
///
/// \tparam   Prop  A property
/// \param    array An array to apply view to
/// \returns  The view corresponding to given array or nullopt if the array
///   cannot be downcast to the array type for the property.
template <typename Prop>
Result<PropertyViewType<Prop>> ConstructPropertyView(arrow::Array* array) {
  using ArrowArrayType = PropertyArrowArrayType<Prop>;
  using ViewType       = PropertyViewType<Prop>;
  auto* t              = dynamic_cast<ArrowArrayType*>(array);

  if (!t) {
    return std::errc::invalid_argument;
  }

  return ViewType::Make(*t);
}

/// ConstructPropertyViews applies ConstructPropertyView to a tuple of
/// properties.
///
/// \tparam   PropTuple a tuple of properties
///
/// \see ConstructPropertyView
template <typename PropTuple>
Result<std::tuple<>> ConstructPropertyViews(const std::vector<arrow::Array*>&,
                                            std::index_sequence<>) {
  return std::tuple<>();
}

template <typename PropTuple, size_t head, size_t... tail>
Result<TupleElements<PropertyViewTuple<PropTuple>, head, tail...>>
ConstructPropertyViews(const std::vector<arrow::Array*>& arrays,
                       std::index_sequence<head, tail...>) {
  using Prop = std::tuple_element_t<head, PropTuple>;
  using View = PropertyViewType<Prop>;

  Result<View> v = ConstructPropertyView<Prop>(arrays[head]);
  if (!v) {
    return v.error();
  }

  auto rest =
      ConstructPropertyViews<PropTuple>(arrays, std::index_sequence<tail...>());
  if (!rest) {
    return rest.error();
  }

  return std::tuple_cat(std::tuple<View>(std::move(v.value())),
                        std::move(rest.value()));
}

template <typename PropTuple>
Result<PropertyViewTuple<PropTuple>>
ConstructPropertyViews(const std::vector<arrow::Array*>& arrays) {
  return ConstructPropertyViews<PropTuple>(
      arrays, std::make_index_sequence<std::tuple_size_v<PropTuple>>());
}

/// PODPropertyView provides a property view over arrow::Arrays of elements
/// with trivial constructors (std::is_trivial) and standard layout
/// (std::is_standard_layout).
///
/// POD types as a concept are deprecated in C++20, but POD so much shorter to
/// say than trivial and standard.
///
/// \tparam T A plain old C datatype type like double or int32_t
template <typename T>
class PODPropertyView {
public:
  using value_type      = T;
  using reference       = T&;
  using const_reference = const T&;

  template <typename U>
  static Result<PODPropertyView> Make(const arrow::NumericArray<U>& array) {
    static_assert(sizeof(typename arrow::NumericArray<U>::value_type) ==
                      sizeof(T),
                  "incompatible types");
    return PODPropertyView(array.data()->template GetMutableValues<T>(1),
                           array.data()->template GetValues<uint8_t>(0));
  }

  static Result<PODPropertyView>
  Make(const arrow::FixedSizeBinaryArray& array) {
    assert(array.byte_width() == sizeof(T));

    return PODPropertyView(array.data()->GetMutableValues<T>(1),
                           array.data()->GetValues<uint8_t>(0));
  }

  bool IsValid(size_t i) const {
    return null_bitmap_ != nullptr && arrow::BitUtil::GetBit(null_bitmap_, i);
  }

  reference GetValue(size_t i) { return values_[i]; }

  const_reference GetValue(size_t i) const { return values_[i]; }

  reference operator[](size_t i) { return GetValue(i); }

  const_reference operator[](size_t i) const { return GetValue(i); }

private:
  PODPropertyView(T* values, const uint8_t* null_bitmap)
      : values_(values), null_bitmap_(null_bitmap) {}

  T* values_;
  const uint8_t* null_bitmap_;
};

/// BooleanPropertyReadOnlyView provides a read-only property view over
/// arrow::Arrays of boolean elements.
class BooleanPropertyReadOnlyView {
public:
  // use uint8_t instead of bool for value_type to avoid std::vector<bool>
  // (std::vector<bool> specialization leads to issues in concurrent writes
  // as well as serialization/deserialization)
  using value_type = uint8_t;

  static Result<BooleanPropertyReadOnlyView>
  Make(const arrow::BooleanArray& array) {
    return BooleanPropertyReadOnlyView(array);
  }

  bool IsValid(size_t i) const { return array_.IsValid(i); }

  value_type GetValue(size_t i) const { return array_.Value(i); }

  value_type operator[](size_t i) const {
    if (!IsValid(i)) {
      return false;
    }
    return GetValue(i);
  }

private:
  BooleanPropertyReadOnlyView(const arrow::BooleanArray& array)
      : array_(array) {}

  const arrow::BooleanArray& array_;
};

/// StringPropertyReadOnlyView provides a read-only property view over
/// arrow::Arrays of string elements.
template <typename OffsetType>
class StringPropertyReadOnlyView {
public:
  using value_type = std::string_view;

  /// Make creates a string property view from a large string array.
  static Result<StringPropertyReadOnlyView>
  Make(const arrow::LargeStringArray& array) {
    return StringPropertyReadOnlyView(
        array.data()->GetMutableValues<uint8_t>(2),
        array.data()->GetValues<OffsetType>(1),
        array.data()->GetValues<uint8_t>(0));
  }

  /// Make creates a string property view from a string array.
  ///
  /// Note that we cannot guarantee all the values will fit in single array
  /// because string array size is limited to 2^32.
  static Result<StringPropertyReadOnlyView>
  Make(const arrow::StringArray& array) {
    return StringPropertyReadOnlyView(
        array.data()->GetMutableValues<uint8_t>(2),
        array.data()->GetValues<OffsetType>(1),
        array.data()->GetValues<uint8_t>(0));
  }

  bool IsValid(size_t i) const {
    return null_bitmap_ != nullptr && arrow::BitUtil::GetBit(null_bitmap_, i);
  }

  value_type GetValue(size_t i) const {
    const OffsetType pos = offsets_[i];
    return value_type(reinterpret_cast<char*>(values_ + pos),
                      offsets_[i + 1] - pos);
  }

  value_type operator[](size_t i) const { return GetValue(i); }

private:
  StringPropertyReadOnlyView(uint8_t* values, const OffsetType* offsets,
                             const uint8_t* null_bitmap)
      : values_(values), offsets_(offsets), null_bitmap_(null_bitmap) {}

  uint8_t* values_{};
  const OffsetType* offsets_{};
  const uint8_t* null_bitmap_{};
};

template <typename T>
struct PODProperty {
  using ArrowType = typename arrow::CTypeTraits<T>::ArrowType;
  using ViewType  = PODPropertyView<T>;
};

struct UInt8Property : public PODProperty<uint8_t> {};

struct UInt16Property : public PODProperty<uint16_t> {};

struct UInt32Property : public PODProperty<uint32_t> {};

struct UInt64Property : public PODProperty<uint64_t> {};

struct BooleanReadOnlyProperty {
  using ArrowType = typename arrow::CTypeTraits<bool>::ArrowType;
  using ViewType  = BooleanPropertyReadOnlyView;
};

struct StringReadOnlyProperty {
  using ArrowType = arrow::StringType;
  using ViewType  = StringPropertyReadOnlyView<int32_t>;
};

struct LargeStringReadOnlyProperty {
  using ArrowType = arrow::LargeStringType;
  using ViewType  = StringPropertyReadOnlyView<int64_t>;
};

} // namespace galois
#endif
