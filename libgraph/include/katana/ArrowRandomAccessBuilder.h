#include <arrow/api.h>

#include "katana/ErrorCode.h"
#include "katana/Logging.h"
#include "katana/NUMAArray.h"
#include "katana/Properties.h"
#include "katana/Result.h"

namespace katana {

namespace internal {

template <typename ArrowType>
class NumberBuilder {
public:
  using value_type = typename ArrowType::c_type;

  katana::Result<void> Resize(size_t length) {
    KATANA_CHECKED(data_.Resize(length));
    data_.bytes_builder()->UnsafeAdvance(
        data_.bytes_builder()->capacity() - data_.bytes_builder()->length());
    valid_.resize(length, false);
    return katana::ResultSuccess();
  }

  // NOTE this operator has side-effects. It can safely be used in two ways:
  // 1) builder[index] = value; where it creates a non-null entry
  // 2) value = builder[index]; ONLY IF option 1 has already used that index
  value_type& operator[](size_t index) {
    KATANA_LOG_DEBUG_VASSERT(
        index < size(), "index: {}, size: {}", index, size());
    valid_[index] = true;
    return reinterpret_cast<value_type*>(data_.mutable_data())[index];
  }

  void UnsetValue(size_t index) {
    KATANA_LOG_DEBUG_ASSERT(index < size());
    valid_[index] = false;
  }

  bool IsValid(size_t index) { return valid_[index]; }

  size_t size() const { return data_.length(); }

  katana::Result<std::shared_ptr<arrow::Array>> Finalize() {
    using ArrayType = typename arrow::TypeTraits<ArrowType>::ArrayType;
    int64_t length = data_.length();

    arrow::TypedBufferBuilder<bool> bitmask_builder;
    KATANA_CHECKED(bitmask_builder.Append(valid_.data(), length));
    std::shared_ptr<arrow::Buffer> bitmask =
        KATANA_CHECKED(bitmask_builder.Finish());
    valid_ = std::vector<uint8_t>(0);

    std::shared_ptr<arrow::Buffer> values = KATANA_CHECKED(data_.Finish());
    data_.Reset();

    return std::make_shared<ArrayType>(
        length, std::move(values), std::move(bitmask));
  }

private:
  arrow::TypedBufferBuilder<value_type> data_;
  std::vector<uint8_t> valid_;
};

template <typename ValueType, typename StorageType, typename ArrowType>
class VectorBackedBuilder {
public:
  using value_type = ValueType;

  katana::Result<void> Resize(size_t length) {
    data_.resize(length);
    valid_.resize(length, false);
    return katana::ResultSuccess();
  }

  // NOTE this operator has side-effects. It can safely be used in two ways:
  // 1) builder[index] = value; where it creates a non-null entry
  // 2) value = builder[index]; ONLY IF option 1 has already used that index
  value_type& operator[](size_t index) {
    KATANA_LOG_DEBUG_VASSERT(
        index < size(), "index: {}, size: {}", index, size());
    valid_[index] = true;
    return reinterpret_cast<ValueType*>(data_.data())[index];
  }

  void UnsetValue(size_t index) {
    KATANA_LOG_DEBUG_ASSERT(index < size());
    valid_[index] = false;
  }

  bool IsValid(size_t index) { return valid_[index]; }

  size_t size() const { return data_.size(); }

  katana::Result<std::shared_ptr<arrow::Array>> Finalize() {
    using ArrowBuilder = typename arrow::TypeTraits<ArrowType>::BuilderType;
    ArrowBuilder builder;
    if (data_.size() > 0) {
      if constexpr (arrow::is_boolean_type<ArrowType>::value) {
        KATANA_CHECKED(
            builder.AppendValues(data_.data(), data_.size(), valid_.data()));
      } else {
        KATANA_CHECKED(builder.AppendValues(data_, valid_.data()));
      }
    }
    data_ = std::vector<StorageType>(0);
    valid_ = std::vector<uint8_t>(0);
    return KATANA_CHECKED(builder.Finish());
  }

private:
  std::vector<StorageType> data_;
  std::vector<uint8_t> valid_;
};

template <typename ArrowType>
struct BuilderTraits;

#define NUMBER(ArrowType)                                                      \
  template <>                                                                  \
  struct BuilderTraits<ArrowType> {                                            \
    using Type = NumberBuilder<ArrowType>;                                     \
  }

NUMBER(arrow::Int8Type);
NUMBER(arrow::UInt8Type);
NUMBER(arrow::Int16Type);
NUMBER(arrow::UInt16Type);
NUMBER(arrow::Int32Type);
NUMBER(arrow::UInt32Type);
NUMBER(arrow::Int64Type);
NUMBER(arrow::UInt64Type);
NUMBER(arrow::FloatType);
NUMBER(arrow::DoubleType);

#undef NUMBER

#define VECTOR_BACKED(ValueType, StorageType, ArrowType)                       \
  template <>                                                                  \
  struct BuilderTraits<ArrowType> {                                            \
    using Type = VectorBackedBuilder<ValueType, StorageType, ArrowType>;       \
  }

VECTOR_BACKED(bool, uint8_t, arrow::BooleanType);
VECTOR_BACKED(std::string, std::string, arrow::StringType);
VECTOR_BACKED(std::string, std::string, arrow::LargeStringType);

#undef VECTOR_BACKED

}  // namespace internal

/// ArrowRandomAccessBuilder encapsulates the concept of building
/// an arrow::Array from <index, value> pairs arriving in unknown order
template <typename ArrowType>
class ArrowRandomAccessBuilder
    : public internal::BuilderTraits<ArrowType>::Type {
public:
  explicit ArrowRandomAccessBuilder(size_t length) {
    KATANA_LOG_ASSERT(this->Resize(length));
  }
};

}  // namespace katana
