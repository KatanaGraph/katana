#include "katana/StorageBackedArrowArray.h"

#include <arrow/compute/api.h>
#include <arrow/io/api.h>
#include <arrow/ipc/api.h>
#include <arrow/type_fwd.h>

#include "katana/FileView.h"
#include "katana/StorageHelpers.h"
#include "storage_operations_generated.h"

template <typename T>
using Result = katana::Result<T>;

template <typename T>
using CopyableResult = katana::CopyableResult<T>;

class katana::StorageBackedArrowArray::DeferredOperation {
public:
  DeferredOperation(const DeferredOperation& no_copy) = delete;
  DeferredOperation& operator=(const DeferredOperation& no_copy) = delete;
  DeferredOperation(DeferredOperation&& no_move) = delete;
  DeferredOperation& operator=(DeferredOperation&& no_move) = delete;

  DeferredOperation() = default;
  virtual ~DeferredOperation();

  virtual Result<void> Apply(StorageBackedArrowArray* to_apply) = 0;
  virtual int64_t LengthDelta() const = 0;
  virtual Result<void> Finalize(StorageBackedArrowArray* to_apply) = 0;
  virtual Result<void> Unload(WriteGroup* wg) = 0;
  virtual Result<void> Persist(
      const URI& storage_prefix, flatbuffers::FlatBufferBuilder* builder,
      std::vector<flatbuffers::Offset<void>>* entries,
      std::vector<uint8_t>* entry_types, WriteGroup* wg) = 0;

  static Result<std::unique_ptr<DeferredOperation>> FromFB(
      const URI& storage_location,
      const fbs::ArrowStorageOperationUnion* fb_op);

protected:
  template <typename ArrayPtr>
  Result<void> Append(StorageBackedArrowArray* base, ArrayPtr to_append) {
    return base->AppendToMaterialized(to_append);
  }

  void SetType(
      StorageBackedArrowArray* base, std::shared_ptr<arrow::DataType> type) {
    base->type_ = std::move(type);
  }

  const std::shared_ptr<arrow::ChunkedArray>& base_array(
      StorageBackedArrowArray* base) {
    return base->materialized_;
  }

  Result<void> AppendEntriesfromOther(
      StorageBackedArrowArray* other, const URI& storage_prefix,
      flatbuffers::FlatBufferBuilder* builder,
      std::vector<flatbuffers::Offset<void>>* entries,
      std::vector<uint8_t>* entry_types, WriteGroup* wg) {
    return other->FillOpEntries(
        storage_prefix, builder, entries, entry_types, wg);
  }
};

// out-of-line
katana::StorageBackedArrowArray::DeferredOperation::~DeferredOperation() =
    default;

namespace {

Result<std::string>
Serialize(const std::shared_ptr<arrow::DataType>& type) {
  // thunt: I'm dubious of storing that arrow IPC type since it could cause
  // our format to break if arrow changes it; I'm not sure that they will
  // but at least we've already committed to parquet so use that instead

  auto buffer_os = KATANA_CHECKED(arrow::io::BufferOutputStream::Create());
  auto tmp_table = arrow::Table::Make(
      arrow::schema({std::make_shared<arrow::Field>("", type)}),
      {std::make_shared<arrow::ChunkedArray>(
          KATANA_CHECKED(arrow::MakeArrayOfNull(type, 0)))});

  KATANA_CHECKED(parquet::arrow::WriteTable(
      *tmp_table, arrow::default_memory_pool(), buffer_os,
      std::numeric_limits<int64_t>::max()));
  auto buffer = KATANA_CHECKED(buffer_os->Finish());
  std::string result(
      reinterpret_cast<const char*>(buffer->data()), buffer->size());
  return result;
}

std::shared_ptr<arrow::DataType>
PromoteToLarge(const std::shared_ptr<arrow::DataType>& type) {
  // "large" types have no parquet analog, we convert them on load because
  // much of the code expects one chunk
  switch (type->id()) {
  case arrow::Type::STRING:
    return arrow::large_utf8();
  default:
    break;
  }
  return type;
}

Result<void>
Deserialize(
    const std::string& payload, std::shared_ptr<arrow::DataType>* type) {
  auto buf_reader =
      std::make_shared<arrow::io::BufferReader>(std::make_shared<arrow::Buffer>(
          reinterpret_cast<const uint8_t*>(payload.c_str()), payload.size()));
  std::unique_ptr<parquet::arrow::FileReader> reader;
  KATANA_CHECKED(parquet::arrow::OpenFile(
      buf_reader, arrow::default_memory_pool(), &reader));
  std::shared_ptr<arrow::Schema> schema;
  KATANA_CHECKED(reader->GetSchema(&schema));
  *type = PromoteToLarge(schema->field(0)->type());
  return katana::ResultSuccess();
}

Result<flatbuffers::Offset<void>>
BuildLoadArrowArray(
    const katana::URI& storage_prefix, const katana::LazyArrowArray& data,
    katana::fbs::ArrayAction action, flatbuffers::FlatBufferBuilder* builder) {
  auto storage_loc_fb = katana::UriToFB(storage_prefix, data.uri(), builder);
  auto serialized_type = KATANA_CHECKED(Serialize(data.type()));
  return katana::fbs::CreateLoadArrowArray(
             *builder, data.length(), builder->CreateString(serialized_type),
             action, storage_loc_fb)
      .Union();
}

Result<flatbuffers::Offset<void>>
BuildLoadArrowArrayAndStorageBackedArrowArray(
    const katana::URI& storage_prefix, const katana::URI& other_uri,
    const katana::LazyArrowArray& data, katana::fbs::ArrayAction action,
    flatbuffers::FlatBufferBuilder* builder) {
  auto storage_loc_fb = katana::UriToFB(storage_prefix, data.uri(), builder);
  auto serialized_type = KATANA_CHECKED(Serialize(data.type()));
  auto other_storage_loc_fb =
      katana::UriToFB(storage_prefix, other_uri, builder);
  auto other_array_loc_fb = katana::fbs::CreateStorageBackedArrowColumn(
      *builder, builder->CreateString("other array"), other_storage_loc_fb);
  return katana::fbs::CreateLoadArrowArrayAndStorageBackedArrowArray(
             *builder, data.length(), builder->CreateString(serialized_type),
             action, storage_loc_fb, other_array_loc_fb)
      .Union();
}

Result<flatbuffers::Offset<void>>
BuildLoadTwoArrowArrays(
    const katana::URI& storage_prefix, const katana::LazyArrowArray& first,
    const katana::LazyArrowArray& second, katana::fbs::ArrayAction action,
    flatbuffers::FlatBufferBuilder* builder) {
  auto first_storage_loc_fb =
      katana::UriToFB(storage_prefix, first.uri(), builder);
  auto second_storage_loc_fb =
      katana::UriToFB(storage_prefix, second.uri(), builder);
  auto first_serialized_type = KATANA_CHECKED(Serialize(first.type()));
  auto second_serialized_type = KATANA_CHECKED(Serialize(second.type()));

  auto first_loc_fb = katana::fbs::CreateLoadArrowArray(
      *builder, first.length(), builder->CreateString(first_serialized_type),
      action, first_storage_loc_fb);
  auto second_loc_fb = katana::fbs::CreateLoadArrowArray(
      *builder, second.length(), builder->CreateString(second_serialized_type),
      action, second_storage_loc_fb);

  return katana::fbs::CreateLoadTwoArrowArrays(
             *builder, first_loc_fb, second_loc_fb)
      .Union();
}

class DeferredAppend
    : public katana::StorageBackedArrowArray::DeferredOperation {
public:
  DeferredAppend(std::shared_ptr<katana::LazyArrowArray> data)
      : data_(std::move(data)) {}

  Result<void> Apply(katana::StorageBackedArrowArray* to_apply) final {
    auto base = base_array(to_apply);
    KATANA_CHECKED(Append(to_apply, KATANA_CHECKED(data_->Get())));
    if (data_->IsOnDisk()) {
      KATANA_CHECKED(data_->Unload());
    }
    return katana::ResultSuccess();
  }

  int64_t LengthDelta() const final { return data_->length(); }

  Result<void> Finalize(katana::StorageBackedArrowArray* to_apply) final {
    if (to_apply->type()->Equals(arrow::null())) {
      SetType(to_apply, data_->type());
    }
    if (!data_->type()->Equals(to_apply->type())) {
      return KATANA_ERROR(
          katana::ErrorCode::NotImplemented,
          "sorry!! missing variant column type support");
    }
    return katana::ResultSuccess();
  }

  Result<void> Unload(katana::WriteGroup* wg) final {
    return data_->Unload(wg);
  }

  Result<void> Persist(
      const katana::URI& storage_prefix,
      flatbuffers::FlatBufferBuilder* builder,
      std::vector<flatbuffers::Offset<void>>* entries,
      std::vector<uint8_t>* entry_types, katana::WriteGroup* wg) final {
    KATANA_CHECKED(data_->Persist(wg));
    entries->emplace_back(KATANA_CHECKED(BuildLoadArrowArray(
        storage_prefix, *data_, katana::fbs::ArrayAction::Append, builder)));
    entry_types->emplace_back(static_cast<uint8_t>(
        katana::fbs::ArrowStorageOperation::LoadArrowArray));
    return katana::ResultSuccess();
  }

private:
  std::shared_ptr<katana::LazyArrowArray> data_;
};

class DeferredAppendOther
    : public katana::StorageBackedArrowArray::DeferredOperation {
public:
  DeferredAppendOther(std::shared_ptr<katana::StorageBackedArrowArray> data)
      : data_(std::move(data)) {}

  Result<void> Apply(katana::StorageBackedArrowArray* to_apply) final {
    return Append(
        to_apply, KATANA_CHECKED(data_->GetArray(/*de_chunk =*/false)));
  }

  int64_t LengthDelta() const final { return data_->length(); }

  Result<void> Finalize(katana::StorageBackedArrowArray* to_apply) final {
    if (to_apply->type()->Equals(arrow::null())) {
      SetType(to_apply, data_->type());
    }
    if (!data_->type()->Equals(to_apply->type())) {
      return KATANA_ERROR(
          katana::ErrorCode::NotImplemented,
          "sorry!! missing variant column type support");
    }
    return katana::ResultSuccess();
  }

  Result<void> Unload(katana::WriteGroup* wg) final {
    return data_->Unload(wg);
  }

  Result<void> Persist(
      const katana::URI& storage_prefix,
      flatbuffers::FlatBufferBuilder* builder,
      std::vector<flatbuffers::Offset<void>>* entries,
      std::vector<uint8_t>* entry_types, katana::WriteGroup* wg) final {
    KATANA_CHECKED(data_->Persist(wg));
    KATANA_CHECKED(AppendEntriesfromOther(
        data_.get(), storage_prefix, builder, entries, entry_types, wg));
    return katana::ResultSuccess();
  }

private:
  std::shared_ptr<katana::StorageBackedArrowArray> data_;
};

class DeferredAppendNulls
    : public katana::StorageBackedArrowArray::DeferredOperation {
public:
  DeferredAppendNulls(int64_t num_nulls) : num_nulls_(num_nulls) {}
  Result<void> Apply(katana::StorageBackedArrowArray* to_apply) final {
    return Append(
        to_apply,
        KATANA_CHECKED(arrow::MakeArrayOfNull(to_apply->type(), num_nulls_)));
  }

  int64_t LengthDelta() const final { return num_nulls_; }

  Result<void> Finalize(katana::StorageBackedArrowArray* /*to_append*/) final {
    return katana::ResultSuccess();
  }

  Result<void> Unload(katana::WriteGroup* /*wg*/) final {
    return katana::ResultSuccess();
  }

  Result<void> Persist(
      const katana::URI& /*storage_prefix*/,
      flatbuffers::FlatBufferBuilder* builder,
      std::vector<flatbuffers::Offset<void>>* entries,
      std::vector<uint8_t>* entry_types, katana::WriteGroup* /*wg*/) final {
    entries->emplace_back(
        katana::fbs::CreateAppendNulls(*builder, num_nulls_).Union());
    entry_types->emplace_back(
        static_cast<uint8_t>(katana::fbs::ArrowStorageOperation::AppendNulls));
    return katana::ResultSuccess();
  }

private:
  int64_t num_nulls_;
};

class DeferredTakeAppendCommon
    : public katana::StorageBackedArrowArray::DeferredOperation {
public:
  DeferredTakeAppendCommon(
      katana::URI storage_location,
      std::shared_ptr<katana::LazyArrowArray> data)
      : storage_location_(std::move(storage_location)),
        data_(std::move(data)) {}

  int64_t LengthDelta() const final { return data_->length(); }

  Result<void> Finalize(katana::StorageBackedArrowArray* /*to_apply*/) final {
    switch (data_->type()->id()) {
    case arrow::Type::INT16:
    case arrow::Type::INT32:
    case arrow::Type::INT64:
    case arrow::Type::UINT8:
    case arrow::Type::UINT16:
    case arrow::Type::UINT32:
    case arrow::Type::UINT64:
      break;
    default:
      return KATANA_ERROR(
          katana::ErrorCode::InvalidArgument,
          "indexes must be some integral type (was given {})",
          std::quoted(data_->type()->name()));
    }
    return katana::ResultSuccess();
  }

protected:
  Result<void> ApplyCommon(
      katana::StorageBackedArrowArray* to_apply,
      const std::function<Result<std::shared_ptr<arrow::ChunkedArray>>(void)>&
          take_source) {
    std::shared_ptr<arrow::ChunkedArray> to_append;

    if (store_result_) {
      to_append = KATANA_CHECKED(data_->Get());
    } else {
      to_append =
          KATANA_CHECKED(arrow::compute::Take(
                             KATANA_CHECKED(take_source()),
                             KATANA_CHECKED(data_->Get()),
                             arrow::compute::TakeOptions::BoundsCheck()))
              .chunked_array();

      // no reason to keep indexes around, we can just store the data
      data_ = std::make_shared<katana::LazyArrowArray>(
          to_append, storage_location_.RandFile("take-result"));
      store_result_ = true;
    }

    KATANA_CHECKED(Append(to_apply, to_append));

    return katana::ResultSuccess();
  }

  katana::URI storage_location_;
  std::shared_ptr<katana::LazyArrowArray> data_;
  bool store_result_{false};
};

class DeferredTakeAppend : public DeferredTakeAppendCommon {
public:
  DeferredTakeAppend(
      katana::URI storage_location,
      std::shared_ptr<katana::LazyArrowArray> data)
      : DeferredTakeAppendCommon(std::move(storage_location), std::move(data)) {
  }

  Result<void> Apply(katana::StorageBackedArrowArray* to_apply) final {
    return ApplyCommon(to_apply, [&]() { return base_array(to_apply); });
  }

  Result<void> Unload(katana::WriteGroup* wg) final {
    return data_->Unload(wg);
  }

  Result<void> Persist(
      const katana::URI& storage_prefix,
      flatbuffers::FlatBufferBuilder* builder,
      std::vector<flatbuffers::Offset<void>>* entries,
      std::vector<uint8_t>* entry_types, katana::WriteGroup* wg) final {
    KATANA_CHECKED(data_->Persist(wg));
    entries->emplace_back(KATANA_CHECKED(BuildLoadArrowArray(
        storage_prefix, *data_,
        store_result_ ? katana::fbs::ArrayAction::Append
                      : katana::fbs::ArrayAction::TakeAndAppend,
        builder)));
    entry_types->emplace_back(static_cast<uint8_t>(
        katana::fbs::ArrowStorageOperation::LoadArrowArray));
    return katana::ResultSuccess();
  }
};

// NB (scober):
// It is unfortunate to have TakeAppend and TakeAppendOther be separate
// operations, but the unified approach causes aliasing issues that required
// deep changes to StorageBackedArrowArray (see Persist, where having source_
// point to the operation log this operation is a part of would cause an
// infinite recursive loop when calling source_->Persist).
class DeferredTakeAppendOther : public DeferredTakeAppendCommon {
public:
  DeferredTakeAppendOther(
      katana::URI storage_location,
      std::shared_ptr<katana::LazyArrowArray> data,
      std::shared_ptr<katana::StorageBackedArrowArray> source)
      : DeferredTakeAppendCommon(std::move(storage_location), std::move(data)),
        source_(std::move(source)) {}

  Result<void> Apply(katana::StorageBackedArrowArray* to_apply) final {
    return ApplyCommon(
        to_apply, [&]() -> Result<std::shared_ptr<arrow::ChunkedArray>> {
          auto datum_max = KATANA_CHECKED(arrow::compute::CallFunction(
              "max", {KATANA_CHECKED(data_->Get())}));
          int64_t max = datum_max.scalar_as<arrow::Int64Scalar>().value;

          return source_->GetSlice(0, max + 1);
        });
  }

  Result<void> Unload(katana::WriteGroup* wg) final {
    KATANA_CHECKED(data_->Unload(wg));
    return source_->Unload(wg);
  }

  Result<void> Persist(
      const katana::URI& storage_prefix,
      flatbuffers::FlatBufferBuilder* builder,
      std::vector<flatbuffers::Offset<void>>* entries,
      std::vector<uint8_t>* entry_types, katana::WriteGroup* wg) final {
    KATANA_CHECKED(data_->Persist(wg));

    if (store_result_) {
      entries->emplace_back(KATANA_CHECKED(BuildLoadArrowArray(
          storage_prefix, *data_, katana::fbs::ArrayAction::Append, builder)));
      entry_types->emplace_back(static_cast<uint8_t>(
          katana::fbs::ArrowStorageOperation::LoadArrowArray));
    } else {
      auto source_uri = KATANA_CHECKED(source_->Persist(wg));
      entries->emplace_back(
          KATANA_CHECKED(BuildLoadArrowArrayAndStorageBackedArrowArray(
              storage_prefix, source_uri, *data_,
              katana::fbs::ArrayAction::TakeAndAppend, builder)));
      entry_types->emplace_back(
          static_cast<uint8_t>(katana::fbs::ArrowStorageOperation::
                                   LoadArrowArrayAndStorageBackedArrowArray));
    }

    return katana::ResultSuccess();
  }

private:
  std::shared_ptr<katana::StorageBackedArrowArray> source_;
};

class DeferredTakeAppendArray : public DeferredTakeAppendCommon {
public:
  DeferredTakeAppendArray(
      katana::URI storage_location,
      std::shared_ptr<katana::LazyArrowArray> indexes,
      std::shared_ptr<katana::LazyArrowArray> source)
      : DeferredTakeAppendCommon(
            std::move(storage_location), std::move(indexes)),
        source_(std::move(source)) {}

  Result<void> Apply(katana::StorageBackedArrowArray* to_apply) final {
    return ApplyCommon(to_apply, [&]() { return source_->Get(); });
  }

  Result<void> Unload(katana::WriteGroup* wg) final {
    KATANA_CHECKED(data_->Unload(wg));
    return source_->Unload(wg);
  }

  Result<void> Persist(
      const katana::URI& storage_prefix,
      flatbuffers::FlatBufferBuilder* builder,
      std::vector<flatbuffers::Offset<void>>* entries,
      std::vector<uint8_t>* entry_types, katana::WriteGroup* wg) final {
    KATANA_CHECKED(data_->Persist(wg));

    if (store_result_) {
      entries->emplace_back(KATANA_CHECKED(BuildLoadArrowArray(
          storage_prefix, *data_, katana::fbs::ArrayAction::Append, builder)));
      entry_types->emplace_back(static_cast<uint8_t>(
          katana::fbs::ArrowStorageOperation::LoadArrowArray));
    } else {
      KATANA_CHECKED(source_->Persist(wg));
      entries->emplace_back(KATANA_CHECKED(BuildLoadTwoArrowArrays(
          storage_prefix, *source_, *data_,
          katana::fbs::ArrayAction::TakeAndAppend, builder)));
      entry_types->emplace_back(static_cast<uint8_t>(
          katana::fbs::ArrowStorageOperation::LoadTwoArrowArrays));
    }

    return katana::ResultSuccess();
  }

private:
  std::shared_ptr<katana::LazyArrowArray> source_;
};

template <typename IntType>
bool
SumIsPositive(IntType old_val, IntType delta) {
  IntType new_val = old_val + delta;
  if (delta >= 0) {
    return new_val >= old_val;
  }
  return new_val >= 0;
}

}  // namespace

Result<std::unique_ptr<katana::StorageBackedArrowArray::DeferredOperation>>
katana::StorageBackedArrowArray::DeferredOperation::FromFB(
    const katana::URI& storage_location,
    const fbs::ArrowStorageOperationUnion* fb_op) {
  if (const auto* ptr = fb_op->AsAppendNulls(); ptr) {
    return std::make_unique<DeferredAppendNulls>(ptr->length);
  }

  if (const auto* ptr = fb_op->AsLoadArrowArray(); ptr) {
    std::shared_ptr<arrow::DataType> type;
    KATANA_CHECKED(Deserialize(ptr->serialized_type, &type));
    auto data = std::make_shared<katana::LazyArrowArray>(
        type, ptr->length,
        KATANA_CHECKED(UriFromFB(storage_location, *ptr->location)));

    switch (ptr->action) {
    case fbs::ArrayAction::Append:
      return std::make_unique<DeferredAppend>(std::move(data));
    case fbs::ArrayAction::TakeAndAppend:
      return std::make_unique<DeferredTakeAppend>(
          storage_location, std::move(data));
    default:
      break;
    }
    return KATANA_ERROR(ErrorCode::AssertionFailed, "unknown array action");
  }

  if (const auto* ptr = fb_op->AsLoadArrowArrayAndStorageBackedArrowArray();
      ptr) {
    std::shared_ptr<arrow::DataType> type;
    KATANA_CHECKED(Deserialize(ptr->serialized_type, &type));
    auto indexes = std::make_shared<katana::LazyArrowArray>(
        type, ptr->length,
        KATANA_CHECKED(UriFromFB(storage_location, *ptr->location)));
    auto source = KATANA_CHECKED(
        katana::StorageBackedArrowArray::FromStorage(KATANA_CHECKED(
            UriFromFB(storage_location, *ptr->storage_backed_array->uri))));
    return std::make_unique<DeferredTakeAppendOther>(
        storage_location, std::move(indexes), std::move(source));
  }

  if (const auto* ptr = fb_op->AsLoadTwoArrowArrays(); ptr) {
    std::shared_ptr<arrow::DataType> first_type;
    std::shared_ptr<arrow::DataType> second_type;
    KATANA_CHECKED(Deserialize(ptr->first->serialized_type, &first_type));
    KATANA_CHECKED(Deserialize(ptr->second->serialized_type, &second_type));
    auto source = std::make_shared<katana::LazyArrowArray>(
        first_type, ptr->first->length,
        KATANA_CHECKED(UriFromFB(storage_location, *ptr->first->location)));
    auto indexes = std::make_shared<katana::LazyArrowArray>(
        second_type, ptr->second->length,
        KATANA_CHECKED(UriFromFB(storage_location, *ptr->second->location)));
    return std::make_unique<DeferredTakeAppendArray>(
        storage_location, std::move(indexes), std::move(source));
  }

  return KATANA_ERROR(ErrorCode::AssertionFailed, "could not handle op type");
}

katana::StorageBackedArrowArray::~StorageBackedArrowArray() = default;

Result<std::shared_ptr<katana::StorageBackedArrowArray>>
katana::StorageBackedArrowArray::Make(
    const URI& storage_location, const std::shared_ptr<LazyArrowArray>& array) {
  return MakeWithOp<DeferredAppend>(storage_location, array->type(), array);
}

Result<std::shared_ptr<katana::StorageBackedArrowArray>>
katana::StorageBackedArrowArray::Make(
    const URI& storage_location, const std::shared_ptr<arrow::DataType>& type,
    int64_t null_count) {
  return MakeWithOp<DeferredAppendNulls>(storage_location, type, null_count);
}

std::future<CopyableResult<std::shared_ptr<katana::StorageBackedArrowArray>>>
katana::StorageBackedArrowArray::FromStorageAsync(const URI& array_file) {
  return std::async(
      std::launch::deferred,
      [=]() -> CopyableResult<std::shared_ptr<StorageBackedArrowArray>> {
        FileView fv;
        KATANA_CHECKED(fv.Bind(array_file.string(), /*resolve=*/true));

        auto storage_location = array_file.DirName();

        flatbuffers::Verifier verifier(fv.ptr<uint8_t>(), fv.size());
        if (!verifier.VerifyBuffer<fbs::StorageBackedArrowArray>()) {
          return KATANA_ERROR(
              ErrorCode::InvalidArgument,
              "file does not appear to contain an array (failed validation)");
        }

        auto fb_sbaa = std::make_unique<fbs::StorageBackedArrowArrayT>();
        flatbuffers::GetRoot<fbs::StorageBackedArrowArray>(fv.ptr<uint8_t>())
            ->UnPackTo(fb_sbaa.get());

        std::shared_ptr<arrow::DataType> type;
        KATANA_CHECKED(Deserialize(fb_sbaa->serialized_type, &type));

        std::list<std::unique_ptr<DeferredOperation>> ops;
        for (const auto& op : fb_sbaa->ops) {
          ops.emplace_back(
              KATANA_CHECKED(DeferredOperation::FromFB(storage_location, &op)));
        }
        auto new_arr = std::shared_ptr<StorageBackedArrowArray>(
            new StorageBackedArrowArray(storage_location, type, nullptr));

        KATANA_CHECKED(new_arr->SetOps(std::move(ops)));

        return new_arr;
      });
}

Result<std::shared_ptr<katana::StorageBackedArrowArray>>
katana::StorageBackedArrowArray::Append(
    const std::shared_ptr<StorageBackedArrowArray>& self,
    const std::shared_ptr<StorageBackedArrowArray>& other) {
  if (other->type()->Equals(arrow::null())) {
    return AppendOp<DeferredAppendNulls>(self, other->length());
  }
  return AppendOp<DeferredAppendOther>(self, other);
}

Result<std::shared_ptr<katana::StorageBackedArrowArray>>
katana::StorageBackedArrowArray::Append(
    const std::shared_ptr<StorageBackedArrowArray>& self,
    const std::shared_ptr<LazyArrowArray>& to_append) {
  if (to_append->type()->Equals(arrow::null())) {
    return AppendOp<DeferredAppendNulls>(self, to_append->length());
  }
  return AppendOp<DeferredAppend>(self, to_append);
}

Result<std::shared_ptr<katana::StorageBackedArrowArray>>
katana::StorageBackedArrowArray::AppendNulls(
    const std::shared_ptr<StorageBackedArrowArray>& self, int64_t null_count) {
  return AppendOp<DeferredAppendNulls>(self, null_count);
}

Result<std::shared_ptr<katana::StorageBackedArrowArray>>
katana::StorageBackedArrowArray::TakeAppend(
    const std::shared_ptr<StorageBackedArrowArray>& self,
    const std::shared_ptr<LazyArrowArray>& indexes) {
  return AppendOp<DeferredTakeAppend>(self, self->storage_location_, indexes);
}

Result<std::shared_ptr<katana::StorageBackedArrowArray>>
katana::StorageBackedArrowArray::TakeAppend(
    const std::shared_ptr<StorageBackedArrowArray>& self,
    const std::shared_ptr<LazyArrowArray>& indexes,
    const std::shared_ptr<StorageBackedArrowArray>& source) {
  return AppendOp<DeferredTakeAppendOther>(
      self, self->storage_location_, indexes, source);
}

Result<std::shared_ptr<katana::StorageBackedArrowArray>>
katana::StorageBackedArrowArray::TakeAppend(
    const std::shared_ptr<StorageBackedArrowArray>& self,
    const std::shared_ptr<LazyArrowArray>& indexes,
    const std::shared_ptr<LazyArrowArray>& source) {
  return AppendOp<DeferredTakeAppendArray>(
      self, self->storage_location_, indexes, source);
}

Result<std::shared_ptr<arrow::ChunkedArray>>
katana::StorageBackedArrowArray::GetArray(bool de_chunk) {
  KATANA_CHECKED(ApplyOp());
  if (de_chunk && materialized_ && materialized_->num_chunks() > 1) {
    materialized_ = std::make_shared<arrow::ChunkedArray>(
        KATANA_CHECKED(arrow::Concatenate(materialized_->chunks())));
  }
  return materialized_;
}

Result<std::shared_ptr<arrow::ChunkedArray>>
katana::StorageBackedArrowArray::GetSlice(
    int64_t offset, int64_t length, bool de_chunk) {
  if (offset < 0 || length < 0) {
    return KATANA_ERROR(
        ErrorCode::InvalidArgument,
        "offset and length must be non-negative (got {} and {})", offset,
        length);
  }
  KATANA_CHECKED(ApplyOp(offset + length));
  if (de_chunk && materialized_ && materialized_->num_chunks() > 1) {
    materialized_ = std::make_shared<arrow::ChunkedArray>(
        KATANA_CHECKED(arrow::Concatenate(materialized_->chunks())));
  }
  return materialized_->Slice(offset, length);
}

Result<void>
katana::StorageBackedArrowArray::Unload(WriteGroup* wg) {
  return CreateOrJoinAsyncGroup(wg, [&](WriteGroup* new_wg) -> Result<void> {
    if (prefix_) {
      KATANA_CHECKED(prefix_->Unload(new_wg));
    }
    for (const auto& op : ops_) {
      KATANA_CHECKED(op->Unload(new_wg));
    }
    materialized_.reset();
    return ResultSuccess();
  });
}

Result<katana::URI>
katana::StorageBackedArrowArray::Persist(WriteGroup* wg) {
  return CreateOrJoinAsyncGroup(wg, [&](WriteGroup* new_wg) -> Result<URI> {
    flatbuffers::FlatBufferBuilder builder;
    std::vector<flatbuffers::Offset<void>> entries;
    std::vector<uint8_t> op_types;
    KATANA_CHECKED(FillOpEntries(
        storage_location_, &builder, &entries, &op_types, new_wg));
    auto entries_offset = builder.CreateVector(entries);
    auto op_types_offset = builder.CreateVector(op_types);
    auto serialized_type_offset =
        builder.CreateString(KATANA_CHECKED(Serialize(type())));

    builder.Finish(fbs::CreateStorageBackedArrowArray(
        builder, length(), serialized_type_offset, op_types_offset,
        entries_offset));

    auto array_file = storage_location_.RandFile("property-column");
    KATANA_CHECKED(PersistFB(builder, array_file, new_wg));

    return array_file;
  });
}

// provide the prefix here because we want to handle the case
// where this buffer came from another import place
Result<void>
katana::StorageBackedArrowArray::FillOpEntries(
    const URI& storage_prefix, flatbuffers::FlatBufferBuilder* builder,
    std::vector<flatbuffers::Offset<void>>* entries,
    std::vector<uint8_t>* types, WriteGroup* wg) {
  if (prefix_) {
    KATANA_CHECKED(
        prefix_->FillOpEntries(storage_prefix, builder, entries, types, wg));
  }
  for (const auto& op : ops_) {
    KATANA_CHECKED(op->Persist(storage_prefix, builder, entries, types, wg));
  }
  return ResultSuccess();
}

katana::StorageBackedArrowArray::StorageBackedArrowArray(
    URI storage_location, std::shared_ptr<arrow::DataType> type,
    std::shared_ptr<StorageBackedArrowArray> prefix)
    : storage_location_(std::move(storage_location)),
      type_(std::move(type)),
      prefix_(std::move(prefix)) {}

Result<void>
katana::StorageBackedArrowArray::SetOps(
    std::list<std::unique_ptr<DeferredOperation>> ops) {
  length_ = prefix_ ? prefix_->length() : 0;
  for (const auto& op : ops) {
    KATANA_CHECKED(op->Finalize(this));
    if (!SumIsPositive(length_, op->LengthDelta())) {
      return KATANA_ERROR(
          ErrorCode::AssertionFailed,
          "requested update would make array too large for int64_t (impossible "
          "for arrow to index)");
    }
    length_ += op->LengthDelta();
  }
  ops_ = std::move(ops);
  return ResultSuccess();
}

template <typename DeferredOpType, typename... Args>
Result<std::shared_ptr<katana::StorageBackedArrowArray>>
katana::StorageBackedArrowArray::MakeCommon(
    const URI& storage_location, const std::shared_ptr<arrow::DataType>& type,
    const std::shared_ptr<StorageBackedArrowArray>& prefix, Args&&... args) {
  auto new_arr = std::shared_ptr<StorageBackedArrowArray>(
      new StorageBackedArrowArray(storage_location, type, prefix));

  std::list<std::unique_ptr<DeferredOperation>> ops;
  ops.emplace_back(
      std::make_unique<DeferredOpType>(std::forward<Args>(args)...));
  KATANA_CHECKED(new_arr->SetOps(std::move(ops)));

  if (!new_arr->type()) {
    return KATANA_ERROR(
        ErrorCode::AssertionFailed, "internal invariant does not hold");
  }

  return MakeResult(std::move(new_arr));
}

template <typename DeferredOpType, typename... Args>
Result<std::shared_ptr<katana::StorageBackedArrowArray>>
katana::StorageBackedArrowArray::AppendOp(
    const std::shared_ptr<StorageBackedArrowArray>& self, Args&&... args) {
  return MakeCommon<DeferredOpType>(
      self->storage_location_, self->type(), self, std::forward<Args>(args)...);
}

template <typename DeferredOpType, typename... Args>
Result<std::shared_ptr<katana::StorageBackedArrowArray>>
katana::StorageBackedArrowArray::MakeWithOp(
    const URI& storage_location, const std::shared_ptr<arrow::DataType>& type,
    Args&&... args) {
  return MakeCommon<DeferredOpType>(
      storage_location, type, nullptr, std::forward<Args>(args)...);
}

Result<void>
katana::StorageBackedArrowArray::ApplyOp(int64_t max_bound) {
  if (IsMaterialized(max_bound)) {
    return ResultSuccess();
  }

  if (prefix_) {
    if (prefix_->type()->Equals(arrow::null())) {
      auto opts = arrow::compute::CastOptions();
      opts.to_type = type();
      materialized_ =
          max_bound < 0
              ? KATANA_CHECKED(
                    arrow::compute::Cast(
                        KATANA_CHECKED(prefix_->GetArray(/*de_chunk=*/false)),
                        opts))
                    .chunked_array()
              : KATANA_CHECKED(arrow::compute::Cast(
                                   KATANA_CHECKED(prefix_->GetSlice(
                                       0, max_bound, /*de_chunk=*/false)),
                                   opts))
                    .chunked_array();
    } else {
      materialized_ =
          max_bound < 0 ? KATANA_CHECKED(prefix_->GetArray(/*de_chunk=*/false))
                        : KATANA_CHECKED(prefix_->GetSlice(
                              0, max_bound, /*de_chunk=*/false));
    }
  }

  for (const auto& op : ops_) {
    auto res = op->Apply(this);
    if (!res) {
      materialized_.reset();
      return res;
    }
    if (IsMaterialized(max_bound)) {
      break;
    }
  }
  return ResultSuccess();
}

Result<void>
katana::StorageBackedArrowArray::AppendToMaterialized(
    const std::shared_ptr<arrow::ChunkedArray>& to_append) {
  if (!materialized_) {
    materialized_ = to_append;
    return ResultSuccess();
  }
  if (!to_append->type()->Equals(type_)) {
    return KATANA_ERROR(
        ErrorCode::AssertionFailed, "internal invariant did not hold");
  }
  auto chunks = materialized_->chunks();
  auto new_chunks = to_append->chunks();
  chunks.insert(chunks.end(), new_chunks.begin(), new_chunks.end());
  materialized_ = std::make_shared<arrow::ChunkedArray>(chunks, type_);
  return ResultSuccess();
}

Result<void>
katana::StorageBackedArrowArray::AppendToMaterialized(
    const std::shared_ptr<arrow::Array>& to_append) {
  return AppendToMaterialized(std::make_shared<arrow::ChunkedArray>(
      std::vector{to_append}, to_append->type()));
}
