#include <algorithm>
#include <random>

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
    if (i % 10) {
      vec.push_back(GetValue<T>(i));
    } else {
      vec.push_back(std::nullopt);
    }
  }
  return vec;
}

template <typename ArrowType>
katana::Result<void>
TestBuilder(size_t size) {
  using T = typename katana::ArrowRandomAccessBuilder<ArrowType>::value_type;
  using ArrayType = typename arrow::TypeTraits<ArrowType>::ArrayType;
  auto canon = GetCanonical<T>(size);

  std::vector<std::pair<size_t, std::optional<T>>> inserts;
  inserts.reserve(size);
  for (size_t i = 0; i < size; ++i) {
    inserts.push_back(std::make_pair(i, canon[i]));
  }
  std::mt19937 rand(20210922);
  std::shuffle(inserts.begin(), inserts.end(), rand);

  katana::ArrowRandomAccessBuilder<ArrowType> sink(size);
  katana::do_all(
      katana::iterate(size_t{0}, size),
      [&](size_t i) {
        auto [idx, data] = inserts[i];
        if (data) {
          sink[idx] = *data;
        }
      },
      katana::no_stats());
  auto array = KATANA_CHECKED(sink.Finalize());
  auto typed = std::dynamic_pointer_cast<ArrayType>(array);

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
Test() {
  if (TestBuilder<ArrowType>(100000))
    return 0;
  else
    return 1;
}

int
main() {
  katana::SharedMemSys Katana_runtime;
  katana::setActiveThreads(8);

  int errors = 0;
  errors += Test<arrow::Int8Type>();
  errors += Test<arrow::UInt8Type>();
  errors += Test<arrow::Int16Type>();
  errors += Test<arrow::UInt16Type>();
  errors += Test<arrow::Int32Type>();
  errors += Test<arrow::UInt32Type>();
  errors += Test<arrow::Int64Type>();
  errors += Test<arrow::UInt64Type>();
  errors += Test<arrow::FloatType>();
  errors += Test<arrow::DoubleType>();
  errors += Test<arrow::BooleanType>();
  errors += Test<arrow::StringType>();
  errors += Test<arrow::LargeStringType>();
  return errors;
}
