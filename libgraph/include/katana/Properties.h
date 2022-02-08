#ifndef KATANA_LIBGRAPH_KATANA_PROPERTIES_H_
#define KATANA_LIBGRAPH_KATANA_PROPERTIES_H_

#include <cassert>
#include <string_view>
#include <utility>

#include <arrow/array.h>
#include <arrow/stl.h>
#include <arrow/type_fwd.h>
#include <arrow/type_traits.h>

#include "katana/ErrorCode.h"
#include "katana/Logging.h"
#include "katana/PODVector.h"
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
/// The way to create a new property is to define a new type that inherits from
/// Property:
///
///   struct Rank: public Property<
///       // type of data as Arrow data type
///       arrow::Int32,
///       // type of view which converts Arrow data to a convenient C type
///       katana::PODPropertyView<int32_t>
///   > { };
///
/// There are convenience classes for common property types:
///
///   struct Rank: public PODProperty<int32_t> {};
///
/// or
///
///   struct Distance: public UInt32Property {};
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
using PropertyValueType = typename PropertyViewType<Prop>::value_type;

template <typename Prop>
using PropertyConstReferenceType =
    typename PropertyViewType<Prop>::const_reference;

namespace internal {
/// Get the mutable values pointer of a mutable ArrayData.
/// This function works around a bug in NumPyBuffer (arrow's wrapper around numpy
/// arrays) which claims to be mutable but returns null from mutable_data().
template <typename T>
T*
GetMutableValuesWorkAround(
    const std::shared_ptr<arrow::ArrayData>& data, int i,
    int64_t absolute_offset) {
  if (data->buffers.size() > static_cast<size_t>(i) &&
      data->buffers[i]->is_mutable()) {
    if (auto r = data->template GetMutableValues<T>(i, absolute_offset); r) {
      return r;
    }
    return const_cast<T*>(data->template GetValues<T>(i, absolute_offset));
  }
  return data->template GetMutableValues<T>(i, absolute_offset);
}

template <typename>
struct PropertyViewTuple;

template <typename... Args>
struct PropertyViewTuple<std::tuple<Args...>> {
  using type = std::tuple<katana::PropertyViewType<Args>...>;
};

}  // namespace internal

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
Result<PropertyViewType<Prop>>
ConstructPropertyView(arrow::Array* array) {
  using ArrowArrayType = PropertyArrowArrayType<Prop>;
  using ViewType = PropertyViewType<Prop>;
  auto* t = dynamic_cast<ArrowArrayType*>(array);

  if (!t) {
    return KATANA_ERROR(
        katana::ErrorCode::TypeError, "Incorrect arrow::Array type: {}",
        array->type()->ToString());
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
    return v.error().template WithContext(
        "failed to convert property with {} remaining", arrays.size());
  }

  auto rest =
      ConstructPropertyViews<PropTuple>(arrays, std::index_sequence<tail...>());
  if (!rest) {
    return rest.error().template WithContext("");
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

template <typename PropTuple>
Result<std::shared_ptr<arrow::Table>>
AllocateTable(
    uint64_t, const std::vector<std::string>&, std::index_sequence<>) {
  return Result<std::shared_ptr<arrow::Table>>(nullptr);
}

template <typename PropTuple, size_t head, size_t... tail>
Result<std::shared_ptr<arrow::Table>>
AllocateTable(
    uint64_t num_rows, const std::vector<std::string>& names,
    std::index_sequence<head, tail...>) {
  using Prop = std::tuple_element_t<head, PropTuple>;

  auto res = Prop::Allocate(num_rows, names[head]);
  if (!res) {
    return res.error();
  }
  auto table = std::move(res.value());

  auto rest =
      AllocateTable<PropTuple>(num_rows, names, std::index_sequence<tail...>());
  if (!rest) {
    return rest.error();
  }

  auto rest_table = std::move(rest.value());

  // TODO(ddn): tail recursion
  // TODO(ddn): metadata
  auto fields = table->fields();
  auto columns = table->columns();

  if (rest_table) {
    for (const auto& field : rest_table->fields()) {
      fields.emplace_back(field);
    }
    for (const auto& col : rest_table->columns()) {
      columns.emplace_back(col);
    }
  }

  return arrow::Table::Make(arrow::schema(fields), columns);
}

template <typename PropTuple>
Result<std::shared_ptr<arrow::Table>>
AllocateTable(uint64_t num_rows, const std::vector<std::string>& names) {
  return AllocateTable<PropTuple>(
      num_rows, names,
      std::make_index_sequence<std::tuple_size_v<PropTuple>>());
}

inline std::shared_ptr<arrow::ChunkedArray>
ApplyBitMask(
    const std::shared_ptr<arrow::ChunkedArray>& chunked_array,
    const std::shared_ptr<arrow::Buffer>& bit_mask) {
  std::shared_ptr<arrow::Array> array = chunked_array->chunk(0);
  std::shared_ptr<arrow::ArrayData> data = array->data()->Copy();

  data->buffers[0] = bit_mask;
  data->null_count = arrow::kUnknownNullCount;

  auto array_ptr = arrow::MakeArray(data);

  KATANA_LOG_DEBUG_ASSERT(array_ptr->ValidateFull().ok());
  std::vector<std::shared_ptr<arrow::Array>> new_chunks;
  new_chunks.emplace_back(array_ptr);

  auto res_new_chunked_array = arrow::ChunkedArray::Make(new_chunks);

  if (res_new_chunked_array.ok()) {
    return res_new_chunked_array.ValueOrDie();
  } else {
    return nullptr;
  }
}

/// This function creates a new table
/// here the bit_mask is applied to the rows in every column
template <typename PropTuple, size_t head, size_t... tail>
Result<std::shared_ptr<arrow::Table>>
AllocateTable(
    uint64_t num_rows, const std::vector<std::string>& names,
    const std::shared_ptr<arrow::Buffer>& bit_mask,
    std::index_sequence<head, tail...>) {
  using Prop = std::tuple_element_t<head, PropTuple>;

  auto res = Prop::Allocate(num_rows, names[head]);
  if (!res) {
    return res.error();
  }
  auto table = std::move(res.value());

  auto rest =
      AllocateTable<PropTuple>(num_rows, names, std::index_sequence<tail...>());
  if (!rest) {
    return rest.error();
  }

  auto rest_table = std::move(rest.value());

  // TODO(ddn): tail recursion
  // TODO(ddn): metadata
  auto fields = table->fields();
  auto columns = table->columns();

  auto new_col = ApplyBitMask(columns[0], bit_mask);
  columns[0] = new_col;

  if (rest_table) {
    for (const auto& field : rest_table->fields()) {
      fields.emplace_back(field);
    }
    for (const auto& col : rest_table->columns()) {
      KATANA_LOG_ASSERT(col->num_chunks() == 1);
      auto new_col = ApplyBitMask(col, bit_mask);
      columns.emplace_back(new_col);
    }
  }

  return arrow::Table::Make(arrow::schema(fields), columns);
}

template <typename PropTuple>
Result<std::shared_ptr<arrow::Table>>
AllocateTable(
    uint64_t num_rows, const std::vector<std::string>& names,
    const std::shared_ptr<arrow::Buffer>& bit_mask) {
  return AllocateTable<PropTuple>(
      num_rows, names, bit_mask,
      std::make_index_sequence<std::tuple_size_v<PropTuple>>());
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
  using value_type = T;
  using reference = T&;
  using const_reference = const T&;

  template <typename U>
  static Result<PODPropertyView> Make(const arrow::NumericArray<U>& array) {
    static_assert(
        sizeof(typename arrow::NumericArray<U>::value_type) == sizeof(T),
        "incompatible types");
    if (array.offset() < 0) {
      return KATANA_ERROR(
          ErrorCode::ArrowError, "offset must be positive, given {}",
          array.offset());
    }
    if (array.data()->buffers.size() <= 1 ||
        !array.data()->buffers[1]->is_mutable()) {
      return KATANA_ERROR(
          ErrorCode::ArrowError, "immutable buffers not supported");
    }
    return PODPropertyView(
        internal::GetMutableValuesWorkAround<T>(array.data(), 1, 0),
        array.data()->template GetValues<uint8_t>(0, 0), array.length(),
        array.offset(), array.null_count());
  }

  static Result<PODPropertyView> Make(
      const arrow::FixedSizeBinaryArray& array) {
    if (array.byte_width() != sizeof(T)) {
      return KATANA_ERROR(
          ErrorCode::ArrowError, "bad byte width of data: {} != {}",
          array.byte_width(), sizeof(T));
    }
    if (array.offset() < 0) {
      return KATANA_ERROR(
          ErrorCode::ArrowError, "offset must be positive, given {}",
          array.offset());
    }
    if (array.data()->buffers.size() <= 1 ||
        !array.data()->buffers[1]->is_mutable()) {
      return KATANA_ERROR(
          ErrorCode::ArrowError, "immutable buffers not supported");
    }
    return PODPropertyView(
        internal::GetMutableValuesWorkAround<T>(array.data(), 1, 0),
        array.data()->template GetValues<uint8_t>(0, 0), array.length(),
        array.offset(), array.null_count());
  }

  bool IsValid(size_t i) const {
    KATANA_LOG_DEBUG_ASSERT(i < length_);
    // if there is no null_bitmap, then we have either all nulls or no nulls
    return null_bitmap_ == nullptr
               ? null_count_ == 0
               : arrow::BitUtil::GetBit(null_bitmap_, i + offset_);
  }

  size_t size() const { return length_; }

  reference GetValue(size_t i) { return values_[i + offset_]; }

  const_reference GetValue(size_t i) const { return values_[i + offset_]; }

  reference operator[](size_t i) { return GetValue(i); }

  const_reference operator[](size_t i) const { return GetValue(i); }

private:
  PODPropertyView(
      T* values, const uint8_t* null_bitmap, size_t length, size_t offset,
      size_t null_count)
      : values_(values),
        null_bitmap_(null_bitmap),
        length_(length),
        offset_(offset),
        null_count_(null_count) {}

  T* values_;
  const uint8_t* null_bitmap_;
  size_t length_, offset_, null_count_;
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

template <typename ArrowT, typename ViewT>
struct Property {
  using ArrowType = ArrowT;
  using ViewType = ViewT;

  static Result<std::shared_ptr<arrow::Table>> Allocate(
      size_t num_rows, const std::string& name) {
    using Builder = typename arrow::TypeTraits<ArrowType>::BuilderType;
    Builder builder;

    KATANA_CHECKED(builder.Reserve(num_rows));
    KATANA_CHECKED_ERROR_CODE(
        builder.AppendEmptyValues(num_rows), katana::ErrorCode::ArrowError,
        "failed to append values");

    std::shared_ptr<arrow::Array> array;
    KATANA_CHECKED_ERROR_CODE(
        builder.Finish(&array), katana::ErrorCode::ArrowError,
        "failed to construct arrow array");

    return arrow::Table::Make(
        arrow::schema({arrow::field(
            name, arrow::TypeTraits<ArrowType>::type_singleton())}),
        {array});
  }
};

/// A PODProperty is a property that has no constructor/destructor, can be
/// copied with memcpy, etc. (i.e., a plain-old data type).
///
/// \tparam T the C type of the backing Arrow property
/// \tparam U (optional) the C type of the viewed value
template <typename T, typename U = T>
struct PODProperty
    : public Property<
          typename arrow::CTypeTraits<T>::ArrowType, PODPropertyView<U>> {};

struct UInt8Property : public PODProperty<uint8_t> {};

struct UInt16Property : public PODProperty<uint16_t> {};

struct UInt32Property : public PODProperty<uint32_t> {};

struct UInt64Property : public PODProperty<uint64_t> {};

template <typename T, typename U = T>
struct AtomicPODProperty : public Property<
                               typename arrow::CTypeTraits<T>::ArrowType,
                               PODPropertyView<std::atomic<U>>> {};

struct BooleanReadOnlyProperty
    : public Property<
          typename arrow::CTypeTraits<bool>::ArrowType,
          BooleanPropertyReadOnlyView> {};

struct StringReadOnlyProperty
    : public Property<
          arrow::StringType, StringPropertyReadOnlyView<arrow::StringArray>> {};

struct LargeStringReadOnlyProperty
    : public Property<
          arrow::LargeStringType,
          StringPropertyReadOnlyView<arrow::LargeStringArray>> {};

template <typename T>
struct StructProperty
    : public Property<arrow::FixedSizeBinaryType, katana::PODPropertyView<T>> {
  static katana::Result<std::shared_ptr<arrow::Table>> Allocate(
      size_t num_rows, const std::string& name) {
    auto res = arrow::FixedSizeBinaryType::Make(sizeof(T));
    if (!res.ok()) {
      return KATANA_ERROR(
          katana::ErrorCode::ArrowError, "failed to make fixed size type: {}",
          res.status());
    }

    auto type = res.ValueOrDie();
    arrow::FixedSizeBinaryBuilder builder(type);
    KATANA_CHECKED(builder.Reserve(num_rows));
    KATANA_CHECKED_ERROR_CODE(
        builder.AppendEmptyValues(num_rows), katana::ErrorCode::ArrowError,
        "failed to append values");

    std::shared_ptr<arrow::Array> array;
    KATANA_CHECKED_ERROR_CODE(
        builder.Finish(&array), katana::ErrorCode::ArrowError,
        "failed to construct arrow array ");

    return katana::Result<std::shared_ptr<arrow::Table>>(
        arrow::Table::Make(arrow::schema({arrow::field(name, type)}), {array}));
  }
};

/// This is a reference to a node's instance of ArrayProperty
///
/// \tparam T the C type of array elements.
template <typename T>
class ArrayRef {
public:
  using value_type = T;
  using iterator = value_type*;
  using const_iterator = const value_type*;

  ArrayRef(T* ptr, size_t size) : data_(ptr), size_(size) {}

  T& operator[](size_t index) { return data_[index]; }
  const T& operator[](size_t index) const { return data_[index]; }
  size_t size() { return size_; }

  T* begin() { return iterator(&data_[0]); }
  const T* begin() const { return const_iterator(&data_[0]); }
  T* end() { return iterator(&data_[size_]); }
  const T* end() const { return const_iterator(&data_[size_]); }

private:
  T* data_;
  size_t size_;
};

/// ArrayPropertyView provides a property view over arrow::LargeListArrays of arrow:Arrays
/// of elements with trivial constructors (std::is_trivial) and standard layout
/// (std::is_standard_layout). (i.e., POD)
///
/// TODO(gurbinder) Currently, only double is supported. Support for other types need to be added.
/// \tparam T the C type of array elements.
/// \tparam N the size of the arrays in arrow::LargeLIstArray
template <typename T, size_t N>
class ArrayPropertyView {
public:
  using value_type = ArrayRef<const T>;
  using reference = ArrayRef<T>;
  using const_reference = const ArrayRef<const T>;

  // TODO(nojan): arrow::DoubleArray is hardcoded here. Add support for other types base on T.
  static Result<ArrayPropertyView> Make(arrow::LargeListArray& array) {
    auto double_array_pointer =
        std::static_pointer_cast<arrow::DoubleArray>(array.values());
    return ArrayPropertyView(
        array, internal::GetMutableValuesWorkAround<T>(
                   double_array_pointer->data(), 1, 0));
  }

  reference GetValue(size_t index) {
    return ArrayRef(ptr_ + array_.value_offset(index), N);
  }
  const_reference GetValue(size_t index) const {
    return ArrayRef(ptr_ + array_.value_offset(index), N);
  }
  reference operator[](size_t index) {
    return ArrayRef(ptr_ + array_.value_offset(index), N);
  }
  const_reference operator[](size_t index) const {
    return ArrayRef(ptr_ + array_.value_offset(index), N);
  }

private:
  ArrayPropertyView(arrow::LargeListArray& array, T* ptr)
      : array_(array), ptr_(ptr) {}
  arrow::LargeListArray& array_;
  T* ptr_;
};

/// An ArrayProperty is a property that is an array of size N of elements
/// with trivial constructors (std::is_trivial) and standard layout
/// (std::is_standard_layout). (i.e., POD)
///
/// TODO(gurbinder) Currently, only double is supported. Support for other types need to be added.
/// \tparam T the C type of array elements.
/// \tparam N the size of the array
template <typename T, size_t N>
struct ArrayProperty {
  using ArrowType = arrow::LargeListType;
  using ViewType = ArrayPropertyView<T, N>;

  static katana::Result<std::shared_ptr<arrow::Table>> Allocate(
      size_t num_rows, const std::string& name) {
    // TODO(nojan): type of arrow::large_list() should be determined by T. arrow::float64 is hardcoded here.
    std::unique_ptr<arrow::ArrayBuilder> builder;
    KATANA_CHECKED(arrow::MakeBuilder(
        arrow::default_memory_pool(), arrow::large_list(arrow::float64()),
        &builder));
    auto outer = dynamic_cast<arrow::LargeListBuilder*>(builder.get());
    // TODO(nojanp): arrow builder type should be determined by T. arrow::DoubleBuilder is hardcoded here.
    auto inner = dynamic_cast<arrow::DoubleBuilder*>(outer->value_builder());

    for (size_t i = 0; i < num_rows; i++) {
      KATANA_CHECKED(outer->Append());
      KATANA_CHECKED(inner->AppendEmptyValues(N));
    }

    std::shared_ptr<arrow::Array> array_of_list_of_double =
        KATANA_CHECKED(builder->Finish());

    return katana::Result<std::shared_ptr<arrow::Table>>(arrow::Table::Make(
        arrow::schema(
            {arrow::field(name, arrow::large_list(arrow::float64()))}),
        {array_of_list_of_double}));
  }
};

/// A mutable view over a FixedSizedBinaryArray with a fixed number of POD types.
/// (POD: elements with trivial constructors (std::is_trivial) and standard layout
/// (std::is_standard_layout))
template <typename PODType, size_t array_size>
class FixedSizeBinaryPODArrayView {
public:
  using value_type = ArrayRef<const PODType>;
  using reference = ArrayRef<PODType>;
  using const_reference = ArrayRef<const PODType>;

  /// Returns a view over the provided FixedSizeBinaryArray given the POD type
  /// contained in the array and the number of elements in the array as template
  /// arguments to the class.
  ///
  /// Note: although the "array" argument is marked "const", the view can make
  /// edits to the underlying memory of the array and edit it.
  static Result<FixedSizeBinaryPODArrayView> Make(
      const arrow::FixedSizeBinaryArray& array) {
    ////////////////////////////////////////////////////////////////////////////
    // sanity checks
    ////////////////////////////////////////////////////////////////////////////
    constexpr size_t total_byte_size = sizeof(PODType) * array_size;
    if (array.byte_width() != total_byte_size) {
      return KATANA_ERROR(
          ErrorCode::ArrowError, "bad byte width of data: {} != {}",
          array.byte_width(), total_byte_size);
    }

    if (array.offset() < 0) {
      return KATANA_ERROR(
          ErrorCode::ArrowError, "offset must be non-negative, given {}",
          array.offset());
    }

    if (array.data()->buffers.size() <= 1 ||
        !array.data()->buffers[1]->is_mutable()) {
      return KATANA_ERROR(
          ErrorCode::ArrowError, "immutable buffers not supported");
    }
    ////////////////////////////////////////////////////////////////////////////

    return FixedSizeBinaryPODArrayView(
        internal::GetMutableValuesWorkAround<uint8_t>(array.data(), 1, 0),
        array.length());
  }

  size_t size() const { return length_; }

  // The functions below return (const) references to the beginning
  // of a particular index's array (See ArrayRef).

  reference GetValue(size_t index) {
    return ArrayRef(ComputePointerLocation(index), array_size);
  }

  const_reference GetValue(size_t index) const {
    return ArrayRef(ComputePointerLocation(index), array_size);
  }

  reference operator[](size_t index) {
    return ArrayRef(ComputePointerLocation(index), array_size);
  }

  const_reference operator[](size_t index) const {
    return ArrayRef(ComputePointerLocation(index), array_size);
  }

private:
  FixedSizeBinaryPODArrayView(uint8_t* ptr, size_t length)
      : ptr_(ptr), length_(length) {}

  /// Pointer to the raw data contained by the FixedSizedBinaryArray.
  uint8_t* ptr_;
  /// Number of elements in the array
  size_t length_;

  /// Returns the correct pointer location for a particular index since
  /// this class will know how many bytes each array element should take
  PODType* ComputePointerLocation(size_t index) {
    constexpr size_t element_byte_size = sizeof(PODType) * array_size;
    return reinterpret_cast<PODType*>(ptr_ + (element_byte_size * index));
  }
};

/// This is an property where each row has some fixed number of POD types with
/// an underlying FixedSizedBinaryArray arrow representation. Constrasts with
/// the ArrayProperty in that it does not use LargeLists (variable size is
/// technically allowed there) and that the current implementation of ArrayProperty
/// only supports doubles.
///
/// @tparam PODType POD type the array will contain
/// @tparam array_size The fixed size of the property array.
template <typename PODType, size_t array_size>
struct FixedSizedBinaryPODArrayProperty
    : public Property<
          arrow::FixedSizeBinaryType,
          FixedSizeBinaryPODArrayView<PODType, array_size>> {
  /// Creates the table that contains a array_size elements of the PODType
  /// for each row as a FixedSizeBinary.
  static katana::Result<std::shared_ptr<arrow::Table>> Allocate(
      size_t num_rows, const std::string& name) {
    // creates the type of the necessary size
    constexpr size_t binary_size = sizeof(PODType) * array_size;
    auto fixed_size_type = KATANA_CHECKED_CONTEXT(
        arrow::FixedSizeBinaryType::Make(binary_size),
        "failed to make fixed size type of size {}", binary_size);

    arrow::FixedSizeBinaryBuilder fixed_sized_binary_builder(fixed_size_type);
    KATANA_CHECKED(fixed_sized_binary_builder.AppendEmptyValues(num_rows));

    std::shared_ptr<arrow::Array> array_of_fixed_size_binaries =
        KATANA_CHECKED_CONTEXT(
            fixed_sized_binary_builder.Finish(),
            "failed to finish fixed size binary builder");

    return arrow::Table::Make(
        arrow::schema({arrow::field(name, fixed_size_type)}),
        {std::move(array_of_fixed_size_binaries)});
  }
};

}  // namespace katana
#endif
