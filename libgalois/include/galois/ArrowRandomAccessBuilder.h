#include <arrow/api.h>

#include "galois/ErrorCode.h"
#include "galois/LargeArray.h"
#include "galois/Logging.h"
#include "galois/Properties.h"
#include "galois/Result.h"

namespace galois {

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
    assert(index < size());
    return ((ValueType*)data_.data())[index];
  }

  size_t size() const { return data_.size(); }

  galois::Result<std::shared_ptr<arrow::Array>> Finalize() const {
    using ArrowBuilder = typename arrow::TypeTraits<ArrowType>::BuilderType;
    ArrowBuilder builder;
    if (data_.size() > 0) {
      if (auto r = builder.AppendValues(data_); !r.ok()) {
        GALOIS_LOG_DEBUG("arrow error: {}", r);
        return galois::ErrorCode::ArrowError;
      }
    }
    std::shared_ptr<arrow::Array> array;
    if (auto r = builder.Finish(&array); !r.ok()) {
      GALOIS_LOG_DEBUG("arrow error: {}", r);
      return galois::ErrorCode::ArrowError;
    }
    return array;
  }

private:
  std::vector<StorageType> data_;
};

template <typename Property>
struct PropertyTypeConfig;

#define NONULL(ValueType, StorageType, PropertyType)                           \
  template <>                                                                  \
  struct PropertyTypeConfig<PropertyType> {                                    \
    using BuilderType = NoNullBuilder<                                         \
        ValueType, StorageType, typename PropertyType::ArrowType>;             \
  }

NONULL(uint8_t, uint8_t, galois::UInt8Property);
NONULL(uint16_t, uint16_t, galois::UInt16Property);
NONULL(uint32_t, uint32_t, galois::UInt32Property);
NONULL(uint64_t, uint64_t, galois::UInt64Property);
NONULL(uint8_t, uint8_t, galois::BooleanReadOnlyProperty);
NONULL(std::string, std::string, galois::StringReadOnlyProperty);
NONULL(std::string, std::string, galois::LargeStringReadOnlyProperty);

#undef NONULL

}  // namespace

/// ArrowRandomAccessBuilder encapsulates the concept of building
/// an arrow::Array from <index, value> pairs arriving in unknown order
/// Functions as a wrapper for NoNullBuilder currently, TODO(danielmawhirter)
template <typename Property>
class ArrowRandomAccessBuilder {
public:
  using BuilderType = typename PropertyTypeConfig<Property>::BuilderType;
  using value_type = typename BuilderType::value_type;

  ArrowRandomAccessBuilder(size_t length) : builder_(length) {}

  void SetValue(size_t index, value_type value) {
    builder_.SetValue(index, value);
  }

  value_type& operator[](size_t index) { return builder_[index]; }

  galois::Result<std::shared_ptr<arrow::Array>> Finalize() {
    return builder_.Finalize();
  }

  size_t size() const { return builder_.size(); }

private:
  BuilderType builder_;
};

}  // namespace galois
