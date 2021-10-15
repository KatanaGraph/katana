#include <arrow/api.h>
#include <boost/atomic/atomic_ref.hpp>

#include "katana/ErrorCode.h"
#include "katana/Logging.h"
#include "katana/NUMAArray.h"
#include "katana/Properties.h"
#include "katana/Result.h"

namespace katana {

namespace internal {

/// VectorBackedBuilder uses std::vector for storage
/// Finalize() makes a copy of the data
/// Thread-safe for concurrent accesses to different indices
/// - Guaranteed by null mask stored as bytes in std::vector
template <typename ValueType, typename StorageType, typename ArrowType_>
class VectorBackedBuilder {
public:
  using ArrowType = ArrowType_;
  using value_type = ValueType;
  using reference = ValueType&;

  VectorBackedBuilder(size_t length) : data_(length), valid_(length, false) {
    static_assert(sizeof(ValueType) == sizeof(StorageType));
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

  katana::Result<std::shared_ptr<arrow::Array>> Finalize() const {
    using ArrowBuilder = typename arrow::TypeTraits<ArrowType>::BuilderType;
    ArrowBuilder builder;
    if (data_.size() > 0) {
      if constexpr (std::is_scalar_v<value_type>) {
        KATANA_CHECKED(
            builder.AppendValues(data_.data(), data_.size(), valid_.data()));
      } else {
        KATANA_CHECKED(builder.AppendValues(data_, valid_.data()));
      }
    }
    return KATANA_CHECKED(builder.Finish());
  }

private:
  std::vector<StorageType> data_;
  std::vector<uint8_t> valid_;
};

/// InPlaceBuilder directly writes to the memory of an arrow::NumericBuilder
/// Finalize() does not copy data, only shared_ptr and null bitmask
template <typename ArrowType_>
class InPlaceBuilder : private arrow::NumericBuilder<ArrowType_> {
public:
  using ArrowType = ArrowType_;
  using ArrowBuilder = typename arrow::NumericBuilder<ArrowType>;
  using typename ArrowBuilder::value_type;

  InPlaceBuilder(size_t length)
      : arrow::NumericBuilder<ArrowType>(), valid_(length, false) {
    auto st = ArrowBuilder::Resize(length);
    KATANA_LOG_ASSERT(st.ok());
  }

  // NOTE this operator has side-effects. It can safely be used in two ways:
  // 1) builder[index] = value; where it creates a non-null entry
  // 2) value = builder[index]; ONLY IF option 1 has already used that index
  value_type& operator[](size_t index) {
    valid_[index] = true;
    return ArrowBuilder::operator[](index);
  }

  void UnsetValue(size_t index) { valid_[index] = false; }

  bool IsValid(size_t index) const { return valid_[index]; }

  size_t size() const { return ArrowBuilder::length(); }

  katana::Result<std::shared_ptr<arrow::Array>> Finalize() {
    KATANA_LOG_ASSERT(size() == 0);
    ArrowBuilder::UnsafeAppendToBitmap(valid_.data(), valid_.size());
    KATANA_LOG_ASSERT(valid_.size() == size());
    valid_ = std::vector<uint8_t>{};
    return KATANA_CHECKED(ArrowBuilder::Finish());
  }

private:
  std::vector<uint8_t> valid_;
};

template <typename ArrowType>
struct VectorBackedBuilderConfig;

#define VECTOR_BACKED_BUILDER(ValueType, StorageType, ArrowType)               \
  template <>                                                                  \
  struct VectorBackedBuilderConfig<ArrowType> {                                \
    using Type = VectorBackedBuilder<ValueType, StorageType, ArrowType>;       \
  }

//arrow::BooleanArray is bit-compacted, promote to byte for thread-safety
VECTOR_BACKED_BUILDER(bool, uint8_t, arrow::BooleanType);

// Intermediate storage is mandatory for non-PODs, as Data size is unknown
VECTOR_BACKED_BUILDER(std::string, std::string, arrow::StringType);
VECTOR_BACKED_BUILDER(std::string, std::string, arrow::LargeStringType);

// TODO(daniel) these should be fine as InPlace
// For some reason, using InPlace makes the partitioner barf
VECTOR_BACKED_BUILDER(int8_t, int8_t, arrow::Int8Type);
VECTOR_BACKED_BUILDER(uint8_t, uint8_t, arrow::UInt8Type);
VECTOR_BACKED_BUILDER(int16_t, int16_t, arrow::Int16Type);
VECTOR_BACKED_BUILDER(uint16_t, uint16_t, arrow::UInt16Type);

VECTOR_BACKED_BUILDER(int32_t, int32_t, arrow::Int32Type);
VECTOR_BACKED_BUILDER(uint32_t, uint32_t, arrow::UInt32Type);
VECTOR_BACKED_BUILDER(int64_t, int64_t, arrow::Int64Type);
VECTOR_BACKED_BUILDER(uint64_t, uint64_t, arrow::UInt64Type);
VECTOR_BACKED_BUILDER(float, float, arrow::FloatType);
VECTOR_BACKED_BUILDER(double, double, arrow::DoubleType);

#undef VECTOR_BACKED_BUILDER

template <typename ArrowType>
struct RandomBuilderTypeConfig;

#define USE_VECTOR_BACKED(ArrowType)                                           \
  template <>                                                                  \
  struct RandomBuilderTypeConfig<ArrowType> {                                  \
    using RandomBuilderType =                                                  \
        typename VectorBackedBuilderConfig<ArrowType>::Type;                   \
  }

#define USE_IN_PLACE(ArrowType)                                                \
  template <>                                                                  \
  struct RandomBuilderTypeConfig<ArrowType> {                                  \
    using RandomBuilderType = InPlaceBuilder<ArrowType>;                       \
  }

USE_VECTOR_BACKED(arrow::BooleanType);
USE_VECTOR_BACKED(arrow::StringType);
USE_VECTOR_BACKED(arrow::LargeStringType);

USE_VECTOR_BACKED(arrow::Int8Type);
USE_VECTOR_BACKED(arrow::UInt8Type);
USE_VECTOR_BACKED(arrow::Int16Type);
USE_VECTOR_BACKED(arrow::UInt16Type);
USE_VECTOR_BACKED(arrow::Int32Type);
USE_VECTOR_BACKED(arrow::UInt32Type);
USE_VECTOR_BACKED(arrow::Int64Type);
USE_VECTOR_BACKED(arrow::UInt64Type);
USE_VECTOR_BACKED(arrow::FloatType);
USE_VECTOR_BACKED(arrow::DoubleType);

/*USE_IN_PLACE(arrow::Int8Type);
USE_IN_PLACE(arrow::UInt8Type);
USE_IN_PLACE(arrow::Int16Type);
USE_IN_PLACE(arrow::UInt16Type);
USE_IN_PLACE(arrow::Int32Type);
USE_IN_PLACE(arrow::UInt32Type);
USE_IN_PLACE(arrow::Int64Type);
USE_IN_PLACE(arrow::UInt64Type);
USE_IN_PLACE(arrow::FloatType);
USE_IN_PLACE(arrow::DoubleType);*/

#undef USE_VECTOR_BACKED
#undef USE_IN_PLACE

}  // namespace internal

/// ArrowRandomAccessBuilder encapsulates the concept of building
/// an arrow::Array from <index, value> pairs arriving in unknown order
/// Length must be known ahead of time
/// Thread-safe for concurrent accesses to different indices
/// All indices are initially Null, using operator[] marks an index as Valid
template <typename ArrowType>
class ArrowRandomAccessBuilder {
public:
  using RandomBuilderType =
      typename internal::RandomBuilderTypeConfig<ArrowType>::RandomBuilderType;
  using value_type = typename RandomBuilderType::value_type;

  ArrowRandomAccessBuilder(size_t length) : builder_(length) {}

  void UnsetValue(size_t index) { builder_.UnsetValue(index); }

  value_type& operator[](size_t index) { return builder_[index]; }

  bool IsValid(size_t index) { return builder_.IsValid(index); }

  katana::Result<std::shared_ptr<arrow::Array>> Finalize() {
    return builder_.Finalize();
  }

  // Deprecated, use the alternate Finalize
  katana::Result<void> Finalize(std::shared_ptr<arrow::Array>* array) {
    *array = KATANA_CHECKED(builder_.Finalize());
    return katana::ResultSuccess();
  }

  size_t size() const { return builder_.size(); }

private:
  RandomBuilderType builder_;
};

}  // namespace katana
