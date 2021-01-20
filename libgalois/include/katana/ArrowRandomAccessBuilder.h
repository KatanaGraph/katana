#include <arrow/api.h>

#include "katana/ErrorCode.h"
#include "katana/LargeArray.h"
#include "katana/Logging.h"
#include "katana/Properties.h"
#include "katana/Result.h"

namespace katana {

namespace {

/// NoNullBuilder uses std::vector for storage
/// Finalize() makes a copy of the data
/// Does not support null values
template <typename ValueType, typename StorageType, typename ArrowType>
class NoNullBuilder {
public:
  using value_type = ValueType;
  using reference = ValueType&;

  NoNullBuilder(size_t length) : data_(length) {
    static_assert(sizeof(ValueType) == sizeof(StorageType));
  }

  reference operator[](size_t index) {
    KATANA_LOG_DEBUG_ASSERT(index < size());
    return static_cast<ValueType*>(data_.data())[index];
  }

  bool IsValid(size_t) { return true; }

  size_t size() const { return data_.size(); }

  katana::Result<std::shared_ptr<arrow::Array>> Finalize() const {
    using ArrowBuilder = typename arrow::TypeTraits<ArrowType>::BuilderType;
    ArrowBuilder builder;
    if (data_.size() > 0) {
      if (auto r = builder.AppendValues(data_); !r.ok()) {
        KATANA_LOG_DEBUG("arrow error: {}", r);
        return katana::ErrorCode::ArrowError;
      }
    }
    std::shared_ptr<arrow::Array> array;
    if (auto r = builder.Finish(&array); !r.ok()) {
      KATANA_LOG_DEBUG("arrow error: {}", r);
      return katana::ErrorCode::ArrowError;
    }
    return array;
  }

private:
  std::vector<StorageType> data_;
};

/// NullableBuilder uses std::vector for storage
/// Finalize() makes a copy of the data
/// Supports null values
template <typename ValueType, typename StorageType, typename ArrowType>
class NullableBuilder {
public:
  using value_type = ValueType;
  using reference = ValueType&;

  NullableBuilder(size_t length) : data_(length), valid_(length, false) {
    static_assert(sizeof(ValueType) == sizeof(StorageType));
  }

  // NOTE this operator has side-effects. It can safely be used in two ways:
  // 1) builder[index] = value; where it creates a non-null entry
  // 2) value = builder[index]; ONLY IF option 1 has already used that index
  reference operator[](size_t index) {
    KATANA_LOG_DEBUG_ASSERT(index < size());
    valid_[index] = true;
    return reinterpret_cast<ValueType*>(data_.data())[index];
  }

  bool IsValid(size_t index) { return valid_[index]; }

  size_t size() const { return data_.size(); }

  katana::Result<void> Finalize(std::shared_ptr<arrow::Array>* array) const {
    using ArrowBuilder = typename arrow::TypeTraits<ArrowType>::BuilderType;
    ArrowBuilder builder;
    if (data_.size() > 0) {
      if constexpr (std::is_scalar_v<value_type>) {
        // TODO(danielmawhirter) find a better way to handle this
        // arrow::NumericBuilder has:
        // AppendValues(value_type*, int64_t, uint8_t*)
        // AppendValues(value_type*, int64_t, vector<bool>)
        // AppendValues(vector<value_type>, vector<bool>)
        if (auto r =
                builder.AppendValues(data_.data(), data_.size(), valid_.data());
            !r.ok()) {
          KATANA_LOG_DEBUG("arrow error: {}", r);
          return katana::ErrorCode::ArrowError;
        }
      } else {
        // TODO(danielmawhirter) find a better way to handle this
        // arrow::BinaryBuilder has:
        // AppendValues(vector<string>, uint8_t*)
        // AppendValues(char**, int64_t, uint8_t*)
        if (auto r = builder.AppendValues(data_, valid_.data()); !r.ok()) {
          KATANA_LOG_DEBUG("arrow error: {}", r);
          return katana::ErrorCode::ArrowError;
        }
      }
    }
    if (auto r = builder.Finish(array); !r.ok()) {
      KATANA_LOG_DEBUG("arrow error: {}", r);
      return katana::ErrorCode::ArrowError;
    }
    return katana::ResultSuccess();
  }

private:
  std::vector<StorageType> data_;
  std::vector<uint8_t> valid_;
};

template <typename ArrowType>
struct ArrowTypeConfig;

#define NULLABLE(ValueType, StorageType, ArrowType)                            \
  template <>                                                                  \
  struct ArrowTypeConfig<ArrowType> {                                          \
    using RandomBuilderType =                                                  \
        NullableBuilder<ValueType, StorageType, ArrowType>;                    \
  }

NULLABLE(int8_t, int8_t, arrow::Int8Type);
NULLABLE(uint8_t, uint8_t, arrow::UInt8Type);
NULLABLE(int16_t, int16_t, arrow::Int16Type);
NULLABLE(uint16_t, uint16_t, arrow::UInt16Type);
NULLABLE(int32_t, int32_t, arrow::Int32Type);
NULLABLE(uint32_t, uint32_t, arrow::UInt32Type);
NULLABLE(int64_t, int64_t, arrow::Int64Type);
NULLABLE(uint64_t, uint64_t, arrow::UInt64Type);
NULLABLE(float, float, arrow::FloatType);
NULLABLE(double, double, arrow::DoubleType);
NULLABLE(bool, uint8_t, arrow::BooleanType);
NULLABLE(std::string, std::string, arrow::StringType);
NULLABLE(std::string, std::string, arrow::LargeStringType);

#undef NULLABLE

}  // namespace

/// ArrowRandomAccessBuilder encapsulates the concept of building
/// an arrow::Array from <index, value> pairs arriving in unknown order
/// Functions as a wrapper for NullableBuilder currently, TODO(danielmawhirter)
template <typename ArrowType>
class ArrowRandomAccessBuilder {
public:
  using RandomBuilderType =
      typename ArrowTypeConfig<ArrowType>::RandomBuilderType;
  using value_type = typename RandomBuilderType::value_type;

  ArrowRandomAccessBuilder(size_t length) : builder_(length) {}

  void SetValue(size_t index, value_type value) {
    builder_.SetValue(index, value);
  }

  value_type& operator[](size_t index) { return builder_[index]; }

  bool IsValid(size_t index) { return builder_.IsValid(index); }

  katana::Result<void> Finalize(std::shared_ptr<arrow::Array>* array) {
    return builder_.Finalize(array);
  }

  katana::Result<std::shared_ptr<arrow::Array>> Finalize() {
    std::shared_ptr<arrow::Array> array;
    auto res = Finalize(&array);
    if (!res) {
      return res.error();
    }
    return array;
  }

  size_t size() const { return builder_.size(); }

private:
  RandomBuilderType builder_;
};

}  // namespace katana
