#include <arrow/api.h>
#include <boost/atomic/atomic_ref.hpp>

#include "katana/ErrorCode.h"
#include "katana/Logging.h"
#include "katana/NUMAArray.h"
#include "katana/Properties.h"
#include "katana/Result.h"

namespace katana {

namespace {

/// VectorBackedBuilder uses std::vector for storage
/// Finalize() makes a copy of the data
/// Thread-safe for concurrent accesses to different indices
/// - Guaranteed by null mask stored as bytes in std::vector
template <typename ValueType, typename StorageType, typename ArrowType>
class VectorBackedBuilder {
public:
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
/// Finalize() does not copy data, only shared_ptr
/// Thread-safe for concurrent accesses to different indices
/// - Guaranteed by boost::atomic_ref to bytes of null bitmask
template <typename ArrowType>
class InPlaceBuilder : private arrow::NumericBuilder<ArrowType> {
public:
  using ArrowBuilder = typename arrow::NumericBuilder<ArrowType>;
  using typename ArrowBuilder::value_type;

  InPlaceBuilder(size_t length) : arrow::NumericBuilder<ArrowType>() {
    auto st = ArrowBuilder::AppendNulls(length);
    KATANA_LOG_ASSERT(st.ok());
  }

  // NOTE this operator has side-effects. It can safely be used in two ways:
  // 1) builder[index] = value; where it creates a non-null entry
  // 2) value = builder[index]; ONLY IF option 1 has already used that index
  value_type& operator[](size_t index) {
    KATANA_LOG_DEBUG_VASSERT(
        index < size(), "index: {}, size: {}", index, size());
    boost::atomic_ref<uint8_t> byte = byte_ref(index);
    byte |= arrow::BitUtil::kBitmask[index % 8];
    return ArrowBuilder::operator[](index);
  }

  void UnsetValue(size_t index) {
    boost::atomic_ref<uint8_t> byte = byte_ref(index);
    byte &= arrow::BitUtil::kFlippedBitmask[index % 8];
  }

  bool IsValid(size_t index) {
    boost::atomic_ref<uint8_t> byte = byte_ref(index);
    return (byte.load() >> (index & 0x07)) & 1;
  }

  size_t size() const { return ArrowBuilder::length(); }

  katana::Result<std::shared_ptr<arrow::Array>> Finalize() {
    return KATANA_CHECKED(ArrowBuilder::Finish());
  }

private:
  boost::atomic_ref<uint8_t> byte_ref(size_t index) {
    uint8_t* ptr = this->null_bitmap_builder_.mutable_data() + (index / 8);
    return boost::atomic_ref<uint8_t>(*ptr);
  }
};

template <typename ArrowType>
struct RandomBuilderTypeConfig;

#define VECTOR_BACKED_BUILDER(ValueType, StorageType, ArrowType)               \
  template <>                                                                  \
  struct RandomBuilderTypeConfig<ArrowType> {                                  \
    using RandomBuilderType =                                                  \
        VectorBackedBuilder<ValueType, StorageType, ArrowType>;                \
  }

#define IN_PLACE_BUILDER(ArrowType)                                            \
  template <>                                                                  \
  struct RandomBuilderTypeConfig<ArrowType> {                                  \
    using RandomBuilderType = InPlaceBuilder<ArrowType>;                       \
  }

// arrow::BooleanArray is bit-compacted, promote to byte for thread-safety
VECTOR_BACKED_BUILDER(bool, uint8_t, arrow::BooleanType);

// Intermediate storage is mandatory for non-PODs, as Data size is unknown
VECTOR_BACKED_BUILDER(std::string, std::string, arrow::StringType);
VECTOR_BACKED_BUILDER(std::string, std::string, arrow::LargeStringType);

// TODO(daniel) these should be fine as InPlace
// For some reason, using InPlace makes the partitioner
// unable to partition Types, stored as uint8_t
// Marking anything smaller than 32-bit as Vector for now
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

/*IN_PLACE_BUILDER(arrow::Int32Type);
IN_PLACE_BUILDER(arrow::UInt32Type);
IN_PLACE_BUILDER(arrow::Int64Type);
IN_PLACE_BUILDER(arrow::UInt64Type);*/
//IN_PLACE_BUILDER(arrow::FloatType);
//IN_PLACE_BUILDER(arrow::DoubleType);

#undef VECTOR_BACKED_BUILDER
#undef IN_PLACE_BUILDER

}  // namespace

/// ArrowRandomAccessBuilder encapsulates the concept of building
/// an arrow::Array from <index, value> pairs arriving in unknown order
/// Length must be known ahead of time
/// Thread-safe for concurrent accesses to different indices
/// All indices are initially Null, using operator[] marks an index as Valid
template <typename ArrowType>
class ArrowRandomAccessBuilder {
public:
  using RandomBuilderType =
      typename RandomBuilderTypeConfig<ArrowType>::RandomBuilderType;
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
