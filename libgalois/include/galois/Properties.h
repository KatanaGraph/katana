#ifndef GALOIS_LIBGALOIS_GALOIS_PROPERTIES_H_
#define GALOIS_LIBGALOIS_GALOIS_PROPERTIES_H_

#include <string_view>
#include <cassert>
#include <utility>

#include <arrow/type_fwd.h>
#include <arrow/type_traits.h>
#include <arrow/array/array_binary.h>

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
  using reference = T&;

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

private:
  PODPropertyView(T* values, const uint8_t* null_bitmap)
      : values_(values), null_bitmap_(null_bitmap) {}

  T* values_;
  const uint8_t* null_bitmap_;
};

/// StringPropertyView provides a property view over arrow::Arrays of string
/// elements.
class StringPropertyView {
public:
  using reference = std::string_view;

  /// Make creates a string property view from a large string array.
  ///
  /// We do not support non-large string arrays because they use int32_t
  /// offsets and we cannot guarantee their values will fit in single array.
  static Result<StringPropertyView> Make(const arrow::LargeStringArray& array) {
    return StringPropertyView(array.data()->GetMutableValues<uint8_t>(2),
                              array.data()->GetValues<int64_t>(1),
                              array.data()->GetValues<uint8_t>(0));
  }

  bool IsValid(size_t i) const {
    return null_bitmap_ != nullptr && arrow::BitUtil::GetBit(null_bitmap_, i);
  }

  reference GetValue(size_t i) {
    const int64_t pos = offsets_[i];
    return reference(reinterpret_cast<char*>(values_ + pos),
                     offsets_[i + 1] - pos);
  }

private:
  StringPropertyView(uint8_t* values, const int64_t* offsets,
                     const uint8_t* null_bitmap)
      : values_(values), offsets_(offsets), null_bitmap_(null_bitmap) {}

  uint8_t* values_{};
  const int64_t* offsets_{};
  const uint8_t* null_bitmap_{};
};

} // namespace galois
#endif
