#include <algorithm>
#include <random>
//#include <omp.h>

#include "katana/ArrowRandomAccessBuilder.h"

template <typename T>
T
GetValue(size_t index) {
  return index;
}

template <>
bool
GetValue<bool>(size_t index) {
  return index % 2;
}

template <>
std::string
GetValue<std::string>(size_t index) {
  std::ostringstream oss;
  oss << index;
  return oss.str();
}

template <typename T>
std::vector<std::optional<T>>
GetCanonical(size_t size) {
  std::vector<std::optional<T>> vec;
  vec.reserve(size);
  for (size_t i = 0; i < size; ++i) {
    if (i % 2) {
      vec.push_back(GetValue<T>(i));
    } else {
      vec.push_back(std::nullopt);
    }
  }
  return vec;
}

template <typename RandomBuilder>
katana::Result<void>
TestBuilder(size_t size, int threads) {
  using T = typename RandomBuilder::value_type;
  using ArrowType = typename RandomBuilder::ArrowType;
  using ArrayType = typename arrow::TypeTraits<ArrowType>::ArrayType;
  auto canon = GetCanonical<T>(size);

  katana::ArrowRandomAccessBuilder<ArrowType> sink(size);

  katana::do_all(
      katana::iterate(0, threads),
      [&](size_t tid) {
        for (size_t i = tid; i < size; i += threads) {
          if (canon[i]) {
            sink[i] = *canon[i];
          }
        }
      },
      katana::no_stats());
  auto array = KATANA_CHECKED(sink.Finalize());
  auto typed = std::dynamic_pointer_cast<ArrayType>(array);

  if ((size_t)array->length() != size) {
    return KATANA_ERROR(
        katana::ErrorCode::AssertionFailed, "expected size of {}, got {}", size,
        array->length());
  }

  for (size_t i = 0; i < size; ++i) {
    bool valid = canon[i].has_value();
    if (typed->IsValid(i) != valid) {
      return KATANA_ERROR(
          katana::ErrorCode::AssertionFailed, "at index {} expected {}, got {}",
          i, valid ? "VALID" : "NULL", typed->IsValid(i) ? "VALID" : "NULL");
    }
    if (valid) {
      T expected = *canon[i];
      T actual;
      if constexpr (std::is_same<T, std::string>::value) {
        actual = typed->GetString(i);
      } else {
        actual = typed->Value(i);
      }
      if (expected != actual) {
        return KATANA_ERROR(
            katana::ErrorCode::AssertionFailed,
            "at index {} expected {}, got {}", i, expected, actual);
      }
    }
  }
  return katana::ResultSuccess();
}

template <typename ArrowType>
int
TestVectorBacked(int threads) {
  KATANA_LOG_DEBUG(
      "testing VectorBacked with type {}",
      arrow::TypeTraits<ArrowType>::type_singleton()->ToString());
  katana::setActiveThreads(threads);
  using Config = katana::internal::VectorBackedBuilderConfig<ArrowType>;
  if (auto res = TestBuilder<typename Config::Type>(1 << 21, threads); res) {
    KATANA_LOG_DEBUG("passed");
    return 0;
  } else {
    KATANA_LOG_ERROR("{}", res.error());
    return 1;
  }
}

template <typename ArrowType>
int
TestInPlace(int threads) {
  KATANA_LOG_DEBUG(
      "testing InPlace with type {}",
      arrow::TypeTraits<ArrowType>::type_singleton()->ToString());
  katana::setActiveThreads(threads);
  using Builder = katana::internal::InPlaceBuilder<ArrowType>;
  if (auto res = TestBuilder<Builder>(1 << 21, threads); res) {
    KATANA_LOG_DEBUG("passed");
    return 0;
  } else {
    KATANA_LOG_ERROR("{}", res.error());
    return 1;
  }
}

int
main() {
  katana::SharedMemSys Katana_runtime;
  int threads = 4;

  int errors = 0;
  errors += TestVectorBacked<arrow::Int8Type>(threads);
  errors += TestVectorBacked<arrow::UInt8Type>(threads);
  errors += TestVectorBacked<arrow::Int16Type>(threads);
  errors += TestVectorBacked<arrow::UInt16Type>(threads);
  errors += TestVectorBacked<arrow::Int32Type>(threads);
  errors += TestVectorBacked<arrow::UInt32Type>(threads);
  errors += TestVectorBacked<arrow::Int64Type>(threads);
  errors += TestVectorBacked<arrow::UInt64Type>(threads);
  errors += TestVectorBacked<arrow::FloatType>(threads);
  errors += TestVectorBacked<arrow::DoubleType>(threads);
  errors += TestVectorBacked<arrow::BooleanType>(threads);
  errors += TestVectorBacked<arrow::StringType>(threads);
  errors += TestVectorBacked<arrow::LargeStringType>(threads);

  errors += TestInPlace<arrow::Int8Type>(threads);
  errors += TestInPlace<arrow::UInt8Type>(threads);
  errors += TestInPlace<arrow::Int16Type>(threads);
  errors += TestInPlace<arrow::UInt16Type>(threads);
  errors += TestInPlace<arrow::Int32Type>(threads);
  errors += TestInPlace<arrow::UInt32Type>(threads);
  errors += TestInPlace<arrow::Int64Type>(threads);
  errors += TestInPlace<arrow::UInt64Type>(threads);
  errors += TestInPlace<arrow::FloatType>(threads);
  errors += TestInPlace<arrow::DoubleType>(threads);

  return errors;
}
