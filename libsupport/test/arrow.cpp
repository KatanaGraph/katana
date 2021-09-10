#include <memory>
#include <type_traits>

#include <arrow/api.h>
#include <arrow/scalar.h>
#include <arrow/type_traits.h>

#include "katana/ArrowVisitor.h"
#include "katana/ErrorCode.h"
#include "katana/Result.h"

namespace {

struct NoCopy {
  NoCopy() = default;
  NoCopy(const NoCopy&) = delete;
  NoCopy& operator=(const NoCopy&) = delete;
};

struct DerivedNoCopy : public NoCopy {};

constexpr arrow::Type::type
GetArrowTypeID(const NoCopy&) {
  return arrow::Type::INT8;
}

template <typename T>
constexpr decltype(auto)
VisitArrowCast(const NoCopy& no_copy) {
  return no_copy;
}

struct NoCopyVisitor : public katana::ArrowVisitor {
  using ResultType = katana::Result<int64_t>;
  using AcceptTypes = std::tuple<
      katana::AcceptNumericArrowTypes, katana::AcceptNumericArrowTypes>;

  template <typename... ArrowTypes>
  ResultType Call(const NoCopy&, const NoCopy&) {
    return 1;
  }

  ResultType AcceptFailed(const NoCopy&, const NoCopy&) {
    return KATANA_ERROR(katana::ErrorCode::ArrowError, "no matching type");
  }
};

struct ManyVisitor : public katana::ArrowVisitor {
  using ResultType = katana::Result<int64_t>;

  using ArgType = std::tuple<arrow::Int8Type>;
  using AcceptTypes = std::tuple<ArgType, ArgType, ArgType, ArgType, ArgType>;

  ResultType AcceptFailed(
      const NoCopy&, const NoCopy&, const NoCopy&, const NoCopy&,
      const NoCopy&) {
    return KATANA_ERROR(katana::ErrorCode::ArrowError, "no matching type");
  }

  template <typename... ArrowTypes>
  ResultType Call(
      const NoCopy&, const NoCopy&, const NoCopy&, const NoCopy&,
      const NoCopy&) {
    return 1;
  }
};

void
TestNoCopy() {
  NoCopy value;
  NoCopyVisitor visitor;

  auto res = katana::VisitArrow(visitor, value, value);
  KATANA_LOG_ASSERT(res);
}

void
TestDerivedNoCopy() {
  DerivedNoCopy value;
  NoCopyVisitor visitor;

  auto res = katana::VisitArrow(visitor, value, value);
  KATANA_LOG_ASSERT(res);
}

void
TestMultipleParameters() {
  NoCopy value;

  ManyVisitor many_visitor;

  // Without pruning instantiations, the following will generally crash a
  // compiler
  auto res =
      katana::VisitArrow(many_visitor, value, value, value, value, value);
  KATANA_LOG_ASSERT(res);
}

void
TestTupleContains() {
  using Dispatcher = katana::internal::ArrowDispatcher;

  using Int8Tuple = std::tuple<arrow::Int8Type>;

  static_assert(Dispatcher::TupleContains<arrow::Int8Type, Int8Tuple>(
      std::make_index_sequence<std::tuple_size_v<Int8Tuple>>()));

  static_assert(!Dispatcher::TupleContains<arrow::ListType, Int8Tuple>(
      std::make_index_sequence<std::tuple_size_v<Int8Tuple>>()));

  static_assert(Dispatcher::TupleContains<
                arrow::Int8Type, katana::AcceptNumericArrowTypes>(
      std::make_index_sequence<
          std::tuple_size_v<katana::AcceptNumericArrowTypes>>()));

  static_assert(!Dispatcher::TupleContains<
                arrow::ListType, katana::AcceptNumericArrowTypes>(
      std::make_index_sequence<
          std::tuple_size_v<katana::AcceptNumericArrowTypes>>()));
}

template <typename T>
std::enable_if_t<arrow::is_string_like_type<T>::value, int>
IsStringLikeTypePatchedNeeded(void*) {
  return false;
}

template <typename T>
std::enable_if_t<!arrow::is_string_like_type<T>::value, int>
IsStringLikeTypePatchedNeeded(void*) {
  return false;
}

template <typename>
int
IsStringLikeTypePatchedNeeded(...) {
  // If this overload is selected, arrow::is_string_like_type<T> is neither
  // true nor false.
  return true;
}

void
TestIsStringLikeTypePatchedNeeded() {
  KATANA_LOG_ASSERT(IsStringLikeTypePatchedNeeded<arrow::BooleanType>(nullptr));
}

}  // namespace

int
main() {
  TestIsStringLikeTypePatchedNeeded();

  TestNoCopy();

  TestDerivedNoCopy();

  TestMultipleParameters();

  TestTupleContains();
}
