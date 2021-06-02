#ifndef KATANA_LIBGALOIS_KATANA_PROPERTIES_H_
#define KATANA_LIBGALOIS_KATANA_PROPERTIES_H_

#include <cassert>
#include <string_view>
#include <utility>

#include <arrow/array.h>
#include <arrow/stl.h>
#include <arrow/type_fwd.h>
#include <arrow/type_traits.h>

#include "katana/ErrorCode.h"
#include "katana/Logging.h"
#include "katana/Result.h"
#include "katana/Traits.h"

namespace katana {

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
/// view on top of their raw data. A PropertyGraph manages raw, untyped
/// data, and a TypedPropertyGraph provides typed property views on top of a
/// PropertyGraph.
///
/// There are two ways of configuring a property for a TypedPropertyGraph.
///
/// The most common way is to create a new type with two nested types:
/// ArrowType and ViewType:
///
///   struct Rank {
///     using ArrowType = arrow::Int32;
///     using ViewType = katana::PODPropertyView<int32_t>;
///   };
///
/// The other way would be to partially specialize PropertyArrowType,
/// PropertyViewType, PropertyArrowArrayType and PropertyReferenceType for your
/// specific property type.
///
/// Once configured, properties can be used as follows:
///
///   PropertyGraph raw_graph = PropertyGraph::Make(....);
///
///   using NodeData = std::tuple<Rank>;
///   using EdgeData = std::tuple<Distance>;
///   using Graph = TypedPropertyGraph<NodeData, EdgeData>;
///
///   Graph graph = Graph::Make(....);
///
///   Graph::node_iterator node = ....;
///
///   int32_t& rank = graph.GetData<Rank>(node);
template <typename Prop>
struct PropertyTraits {
  using ArrowType = typename Prop::ArrowType;
  using ViewType = typename Prop::ViewType;
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

template <typename Prop>
using PropertyConstReferenceType =
    typename PropertyViewType<Prop>::const_reference;

namespace internal {

template <typename>
struct PropertyViewTuple;

template <typename... Args>
struct PropertyViewTuple<std::tuple<Args...>> {
  using type = std::tuple<katana::PropertyViewType<Args>...>;
};

template <typename>
struct PropertyArrowTuple;

template <typename... Args>
struct PropertyArrowTuple<std::tuple<Args...>> {
  using type = std::tuple<
      typename arrow::TypeTraits<katana::PropertyArrowType<Args>>::CType...>;
};

}  // namespace internal

/// PropertyViewTuple applies PropertyViewType to a tuple of properties.
template <typename T>
using PropertyViewTuple = typename internal::PropertyViewTuple<T>::type;

/// PropertyArrowTuple applies arrow::TypeTraits<T::ArrowType>::CType
/// to a tuple of properties
template <typename T>
using PropertyArrowTuple = typename internal::PropertyArrowTuple<T>::type;

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
Result<PropertyViewType<Prop>>
ConstructPropertyView(arrow::Array* array) {
  using ArrowArrayType = PropertyArrowArrayType<Prop>;
  using ViewType = PropertyViewType<Prop>;
  auto* t = dynamic_cast<ArrowArrayType*>(array);

  if (!t) {
    return katana::ErrorCode::TypeError;
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
Result<std::tuple<>>
ConstructPropertyViews(
    const std::vector<arrow::Array*>&, std::index_sequence<>) {
  return Result<std::tuple<>>(std::tuple<>());
}

template <typename PropTuple, size_t head, size_t... tail>
Result<TupleElements<PropertyViewTuple<PropTuple>, head, tail...>>
ConstructPropertyViews(
    const std::vector<arrow::Array*>& arrays,
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

  return std::tuple_cat(
      std::tuple<View>(std::move(v.value())), std::move(rest.value()));
}

template <typename PropTuple>
Result<PropertyViewTuple<PropTuple>>
ConstructPropertyViews(const std::vector<arrow::Array*>& arrays) {
  return ConstructPropertyViews<PropTuple>(
      arrays, std::make_index_sequence<std::tuple_size_v<PropTuple>>());
}

namespace {
/// Get the mutable values pointer of a mutable ArrayData.
/// This function works around a bug in NumPyBuffer (arrow's wrapper around numpy
/// arrays) which claims to be mutable but returns null from mutable_data().
template <typename T>
T*
GetMutableValuesWorkAround(
    const std::shared_ptr<arrow::ArrayData>& data, int i,
    int64_t absolute_offset) {
  if (data->buffers.size() > (size_t)i && data->buffers[i]->is_mutable()) {
    if (auto r = data->template GetMutableValues<T>(i, absolute_offset); r) {
      return r;
    } else {
      return const_cast<T*>(data->template GetValues<T>(i, absolute_offset));
    }
  } else {
    return data->template GetMutableValues<T>(i, absolute_offset);
  }
}
}  // namespace

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
  using value_type = T;
  using reference = T&;
  using const_reference = const T&;

  template <typename U>
  static Result<PODPropertyView> Make(const arrow::NumericArray<U>& array) {
    static_assert(
        sizeof(typename arrow::NumericArray<U>::value_type) == sizeof(T),
        "incompatible types");
    if (array.offset() < 0) {
      KATANA_LOG_DEBUG("arrow error: Offset not supported");
      return ErrorCode::ArrowError;
    }
    if (array.data()->buffers.size() <= 1 ||
        !array.data()->buffers[1]->is_mutable()) {
      KATANA_LOG_DEBUG("arrow error: immutable buffers not supported");
      return ErrorCode::ArrowError;
    }
    return PODPropertyView(
        GetMutableValuesWorkAround<T>(array.data(), 1, 0),
        array.data()->template GetValues<uint8_t>(0, 0), array.length(),
        array.offset());
  }

  static Result<PODPropertyView> Make(
      const arrow::FixedSizeBinaryArray& array) {
    if (array.byte_width() != sizeof(T)) {
      KATANA_LOG_DEBUG(
          "arrow error: bad byte width of data: {} != {}", array.byte_width(),
          sizeof(T));
      return ErrorCode::ArrowError;
    }
    if (array.offset() < 0) {
      KATANA_LOG_DEBUG("arrow error: Offset not supported");
      return ErrorCode::ArrowError;
    }
    if (array.data()->buffers.size() <= 1 ||
        !array.data()->buffers[1]->is_mutable()) {
      KATANA_LOG_DEBUG("arrow error: immutable buffers not supported");
      return ErrorCode::ArrowError;
    }
    return PODPropertyView(
        GetMutableValuesWorkAround<T>(array.data(), 1, 0),
        array.data()->template GetValues<uint8_t>(0, 0), array.length(),
        array.offset());
  }

  bool IsValid(size_t i) const {
    KATANA_LOG_DEBUG_ASSERT(i < length_);
    return null_bitmap_ == nullptr ||
           arrow::BitUtil::GetBit(null_bitmap_, i + offset_);
  }

  reference GetValue(size_t i) { return values_[i + offset_]; }

  const_reference GetValue(size_t i) const { return values_[i + offset_]; }

  reference operator[](size_t i) { return GetValue(i); }

  const_reference operator[](size_t i) const { return GetValue(i); }

private:
  PODPropertyView(
      T* values, const uint8_t* null_bitmap, size_t length, size_t offset)
      : values_(values),
        null_bitmap_(null_bitmap),
        length_(length),
        offset_(offset) {}

  T* values_;
  const uint8_t* null_bitmap_;
  size_t length_, offset_;
};

/// BooleanPropertyReadOnlyView provides a read-only property view over
/// arrow::Arrays of boolean elements.
class KATANA_EXPORT BooleanPropertyReadOnlyView {
public:
  // use uint8_t instead of bool for value_type to avoid std::vector<bool>
  // (std::vector<bool> specialization leads to issues in concurrent writes
  // as well as serialization/deserialization)
  using value_type = uint8_t;

  static Result<BooleanPropertyReadOnlyView> Make(
      const arrow::BooleanArray& array);

  bool IsValid(size_t i) const {
    KATANA_LOG_DEBUG_ASSERT(i < (size_t)array_.length());
    return array_.IsValid(i);
  }

  value_type GetValue(size_t i) const {
    KATANA_LOG_DEBUG_ASSERT(IsValid(i));
    return array_.Value(i);
  }

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
/// arrow::Arrays of string elements
/// (i.e., arrow::StringArray or arrow::LargeStringArray).
template <typename ArrowArrayType>
class StringPropertyReadOnlyView {
public:
  using value_type = std::string;

  static Result<StringPropertyReadOnlyView> Make(const ArrowArrayType& array) {
    return StringPropertyReadOnlyView(array);
  }

  bool IsValid(size_t i) const { return array_.IsValid(i); }

  value_type GetValue(size_t i) const {
    KATANA_LOG_DEBUG_ASSERT(IsValid(i));
    return array_.GetString(i);
  }

  value_type operator[](size_t i) const {
    if (!IsValid(i)) {
      return value_type{};
    }
    return GetValue(i);
  }

private:
  StringPropertyReadOnlyView(const ArrowArrayType& array) : array_(array) {}

  const ArrowArrayType& array_;
};

template <typename T>
struct PODProperty {
  using ArrowType = typename arrow::CTypeTraits<T>::ArrowType;
  using ViewType = PODPropertyView<T>;
};

struct UInt8Property : public PODProperty<uint8_t> {};

struct UInt16Property : public PODProperty<uint16_t> {};

struct UInt32Property : public PODProperty<uint32_t> {};

struct UInt64Property : public PODProperty<uint64_t> {};

struct BooleanReadOnlyProperty {
  using ArrowType = typename arrow::CTypeTraits<bool>::ArrowType;
  using ViewType = BooleanPropertyReadOnlyView;
};

struct StringReadOnlyProperty {
  using ArrowType = arrow::StringType;
  using ViewType = StringPropertyReadOnlyView<arrow::StringArray>;
};

struct LargeStringReadOnlyProperty {
  using ArrowType = arrow::LargeStringType;
  using ViewType = StringPropertyReadOnlyView<arrow::LargeStringArray>;
};

template <typename Props>
Result<std::shared_ptr<arrow::Table>>
AllocateTable(uint64_t num_rows, const std::vector<std::string>& names) {
  constexpr auto num_tuple_elem = std::tuple_size<Props>::value;
  static_assert(num_tuple_elem != 0);
  std::shared_ptr<arrow::Table> table;
  std::vector<katana::PropertyArrowTuple<Props>> rows(num_rows);
  KATANA_LOG_ASSERT(names.size() == num_tuple_elem);
  // TODO(gill): Replace this with NUMA allocated buffers.
  if (auto r = arrow::stl::TableFromTupleRange(
          arrow::default_memory_pool(), std::move(rows), names, &table);
      !r.ok()) {
    KATANA_LOG_DEBUG("arrow error: {}", r);
    return Result<std::shared_ptr<arrow::Table>>(katana::ErrorCode::ArrowError);
  }
  return Result<std::shared_ptr<arrow::Table>>(table);
}

}  // namespace katana
#endif
