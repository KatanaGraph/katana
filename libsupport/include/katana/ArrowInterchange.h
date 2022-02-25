#ifndef KATANA_LIBSUPPORT_KATANA_ARROWINTERCHANGE_H_
#define KATANA_LIBSUPPORT_KATANA_ARROWINTERCHANGE_H_

#include <arrow/stl.h>
#include <arrow/type_traits.h>

#include "katana/ErrorCode.h"
#include "katana/Logging.h"
#include "katana/Result.h"

/// We have two strategies for Arrow conversion.  One uses
/// arrow::stl::TableFromTupleRange, the other uses Builders. TableFromTupleRange is
/// good for statically typed data present in an STL collection.  Builders are good
/// when static types are not known or data is being generated.
///
/// NB: The schema for a table returned by TableFromTupleRange will contain
/// "not null."  We make the type nullable in VectorToArrowTable.
///
/// \file

namespace katana {

/// Perform a safe cast from \param gen_array to \tparam ArrowArrayType
/// calls the array's `View()` member first to make sure cast is safe.
template <typename ArrowArrayType>
Result<std::shared_ptr<ArrowArrayType>>
ViewCast(const std::shared_ptr<arrow::Array>& gen_array) {
  return std::static_pointer_cast<ArrowArrayType>(KATANA_CHECKED(
      gen_array->View(arrow::TypeTraits<
                      typename ArrowArrayType::TypeClass>::type_singleton())));
}

template <typename T>
std::vector<T>*
SingleView(std::vector<std::tuple<T>>* v) {
  auto* r = reinterpret_cast<std::vector<T>*>(v);
  static_assert(
      sizeof(typename std::decay_t<decltype(*v)>::value_type) ==
      sizeof(typename std::decay_t<decltype(*r)>::value_type));
  return r;
}

template <typename T>
const std::vector<T>*
SingleView(const std::vector<std::tuple<T>>* v) {
  auto* r = reinterpret_cast<const std::vector<T>*>(v);
  static_assert(
      sizeof(typename std::decay_t<decltype(*v)>::value_type) ==
      sizeof(typename std::decay_t<decltype(*r)>::value_type));
  return r;
}

template <typename T>
std::vector<std::tuple<T>>*
TupleView(std::vector<T>* v) {
  auto* r = reinterpret_cast<std::vector<std::tuple<T>>*>(v);
  static_assert(
      sizeof(typename std::decay_t<decltype(*v)>::value_type) ==
      sizeof(typename std::decay_t<decltype(*r)>::value_type));
  return r;
}

template <typename T>
const std::vector<std::tuple<T>>*
TupleView(const std::vector<T>* v) {
  auto* r = reinterpret_cast<const std::vector<std::tuple<T>>*>(v);
  static_assert(
      sizeof(typename std::decay_t<decltype(*v)>::value_type) ==
      sizeof(typename std::decay_t<decltype(*r)>::value_type));
  return r;
}

template <typename T>
katana::Result<std::vector<T>>
UnmarshalVector(const std::shared_ptr<arrow::ChunkedArray>& source) {
  using Row = std::tuple<T>;

  arrow::compute::CastOptions cast_options;
  arrow::compute::ExecContext ctx;

  std::shared_ptr<arrow::Schema> schema =
      arrow::stl::SchemaFromTuple<Row>::MakeSchema({"column1"});

  std::vector<Row> dest(source->length());

  std::shared_ptr<arrow::Table> table = arrow::Table::Make(schema, {source});

  if (auto r =
          arrow::stl::TupleRangeFromTable(*table, cast_options, &ctx, &dest);
      !r.ok()) {
    return KATANA_ERROR(
        katana::ErrorCode::ArrowError, "converting buffer to vector: {}", r);
  }
  return std::vector<T>(std::move(*SingleView(&dest)));
}

template <typename T>
katana::Result<std::shared_ptr<arrow::ChunkedArray>>
MarshalVector(const std::vector<T>& source) {
  using Row = std::tuple<T>;

  auto* pool = arrow::default_memory_pool();

  const std::vector<Row>* source_view = TupleView(&source);

  std::shared_ptr<arrow::Table> table;
  if (auto r = arrow::stl::TableFromTupleRange(
          pool, *source_view, {"column1"}, &table);
      !r.ok()) {
    return KATANA_ERROR(
        katana::ErrorCode::ArrowError, "converting vector to arrow: {}", r);
  }
  return katana::Result<std::shared_ptr<arrow::ChunkedArray>>(table->column(0));
}

template <typename T>
katana::Result<std::shared_ptr<arrow::Table>>
VectorToArrowTable(const std::string& name, const std::vector<T>& source) {
  using Row = std::tuple<T>;

  auto* pool = arrow::default_memory_pool();

  const std::vector<Row>* source_view = TupleView(&source);

  std::shared_ptr<arrow::Table> table;
  if (auto r =
          arrow::stl::TableFromTupleRange(pool, *source_view, {name}, &table);
      !r.ok()) {
    return KATANA_ERROR(
        katana::ErrorCode::ArrowError, "converting to arrow: {}", r);
  }
  // Jump through hoops to make the type nullable even though we are not using
  // a builder and there are no null values.  Documented to be zero copy.
  auto nullable_field = table->schema()->field(0)->WithNullable(true);
  auto nullable_table = table->SetColumn(0, nullable_field, table->column(0));
  if (!nullable_table.ok()) {
    return KATANA_ERROR(
        katana::ErrorCode::ArrowError, "setting arrow column attributes: {}",
        nullable_table.status());
  }
  return katana::Result<std::shared_ptr<arrow::Table>>(
      nullable_table.ValueOrDie());
}

template <typename T>
katana::Result<void>
UnmarshalVectorOfVectors(
    const std::vector<std::shared_ptr<arrow::ChunkedArray>>& source,
    std::vector<std::vector<T>>* dest) {
  KATANA_LOG_VASSERT(
      source.size() == dest->size(), "source: {} dest: {}", source.size(),
      dest->size());

  for (size_t i = 0; i < source.size(); ++i) {
    auto res = UnmarshalVector<T>(source[i]);
    if (!res) {
      return res.error().WithContext("converting chunk {}", i);
    }
    dest->at(i) = std::move(res.value());
  }
  return katana::ResultSuccess();
}

template <typename T>
katana::Result<std::vector<std::shared_ptr<arrow::ChunkedArray>>>
MarshalVectorOfVectors(const std::vector<std::vector<T>>& source) {
  std::vector<std::shared_ptr<arrow::ChunkedArray>> dest;

  for (const auto& vec : source) {
    auto res = MarshalVector(vec);
    if (!res) {
      return res.error().WithContext("converting vector of vectors to array");
    }
    dest.emplace_back(res.value());
  }
  return katana::Result<std::vector<std::shared_ptr<arrow::ChunkedArray>>>(
      std::vector<std::shared_ptr<arrow::ChunkedArray>>(std::move(dest)));
}

//////////////////////////////////////////////////////////
// Code below uses builders

/// BuildArray copies the input data into an arrow array
template <typename T>
std::shared_ptr<arrow::Array>
BuildArray(std::vector<T>& data) {
  using Builder = typename arrow::CTypeTraits<T>::BuilderType;

  Builder builder;
  if (!data.empty()) {
    auto append_status = builder.AppendValues(data);
    KATANA_LOG_ASSERT(append_status.ok());
  }
  std::shared_ptr<arrow::Array> array;
  auto finish_status = builder.Finish(&array);
  KATANA_LOG_ASSERT(finish_status.ok());
  return array;
}

struct ColumnOptions {
  std::string name;
  size_t chunk_size{~size_t{0}};
  bool ascending_values{false};
};

/// TableBuilder builds tables with various data types but with a fixed value
/// distribution. It is mainly for making inputs for testing and benchmarking.
class TableBuilder {
  size_t size_{0};
  std::vector<std::shared_ptr<arrow::ChunkedArray>> columns_;
  std::vector<std::shared_ptr<arrow::Field>> fields_;

public:
  TableBuilder(size_t size) : size_(size) {}

  template <typename T>
  void AddColumn(const ColumnOptions& options);

  template <typename T>
  void AddColumn() {
    return AddColumn<T>(ColumnOptions());
  }

  std::shared_ptr<arrow::Table> Finish() {
    auto ret = arrow::Table::Make(arrow::schema(fields_), columns_);
    columns_.clear();
    fields_.clear();
    return ret;
  }
};

template <typename T>
void
TableBuilder::AddColumn(const ColumnOptions& options) {
  using Builder = typename arrow::CTypeTraits<T>::BuilderType;
  using ArrowType = typename arrow::CTypeTraits<T>::ArrowType;

  std::vector<std::shared_ptr<arrow::Array>> chunks;

  std::vector<T> data;
  T value{};

  for (size_t chunk_index = 0, idx = 0; idx < size_; ++idx, ++value) {
    if (options.ascending_values) {
      data.emplace_back(value);
    } else {
      data.emplace_back(1);
    }
    bool last_in_chunk = (chunk_index + 1 >= options.chunk_size);
    bool last = (idx + 1 >= size_);

    if (!last_in_chunk && !last) {
      ++chunk_index;
      continue;
    }

    Builder builder;
    auto append_status = builder.AppendValues(data);
    KATANA_LOG_ASSERT(append_status.ok());

    data.clear();

    std::shared_ptr<arrow::Array> array;
    auto finish_status = builder.Finish(&array);
    KATANA_LOG_ASSERT(finish_status.ok());

    chunks.emplace_back(std::move(array));
  }

  std::string name = options.name;
  if (name.empty()) {
    name = std::to_string(fields_.size());
  }

  fields_.emplace_back(arrow::field(name, std::make_shared<ArrowType>()));
  columns_.emplace_back(std::make_shared<arrow::ChunkedArray>(chunks));
}

/// Return a ChunkeArray of Nulls of the given type and length
KATANA_EXPORT Result<std::shared_ptr<arrow::ChunkedArray>> NullChunkedArray(
    const std::shared_ptr<arrow::DataType>& type, int64_t length);

KATANA_EXPORT std::shared_ptr<arrow::Table> MakeEmptyArrowTable();

/// Print the differences between two ChunkedArrays only using
/// about approx_total_characters
KATANA_EXPORT void DiffFormatTo(
    fmt::memory_buffer& buf, const std::shared_ptr<arrow::ChunkedArray>& a0,
    const std::shared_ptr<arrow::ChunkedArray>& a1,
    size_t approx_total_characters = 150);

/// Estimate the amount of memory this array is using
/// n.b. Estimate is best effort when array is a slice or a variable type like
///   large_string; it will be an upper bound in those cases
KATANA_EXPORT uint64_t
ApproxArrayMemUse(const std::shared_ptr<arrow::Array>& array);

/// Estimate the amount of memory this table is using
/// n.b. Estimate is best effort when an array is a slice or a variable type like
///   large_string; it will be an upper bound in those cases
KATANA_EXPORT uint64_t
ApproxTableMemUse(const std::shared_ptr<arrow::Table>& table);

KATANA_EXPORT Result<std::shared_ptr<arrow::Table>> TakeRows(
    const std::shared_ptr<arrow::Table>& original,
    const std::shared_ptr<arrow::BooleanArray>& picker);

}  // namespace katana

#endif
