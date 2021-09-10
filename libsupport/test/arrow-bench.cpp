#include <memory>
#include <random>
#include <tuple>
#include <type_traits>
#include <utility>

#include <arrow/api.h>
#include <arrow/scalar.h>
#include <arrow/type.h>
#include <arrow/type_traits.h>
#include <benchmark/benchmark.h>

#include "katana/ArrowVisitor.h"
#include "katana/ErrorCode.h"
#include "katana/Random.h"
#include "katana/Result.h"

namespace {

void
MakeArguments(benchmark::internal::Benchmark* b) {
  for (long size : {1024, 64 * 1024, 1024 * 1024}) {
    b->Args({size});
  }
}

std::vector<std::unique_ptr<arrow::Scalar>>
MakeInput(long size) {
  std::vector<std::unique_ptr<arrow::Scalar>> ret;

  std::vector<std::function<std::unique_ptr<arrow::Scalar>()>> generators{
      []() { return std::make_unique<arrow::Int64Scalar>(1); },
      []() { return std::make_unique<arrow::Int32Scalar>(1); },
      []() { return std::make_unique<arrow::Int16Scalar>(1); },
      []() { return std::make_unique<arrow::Int8Scalar>(1); },
      []() { return std::make_unique<arrow::UInt64Scalar>(1); },
      []() { return std::make_unique<arrow::UInt32Scalar>(1); },
      []() { return std::make_unique<arrow::UInt16Scalar>(1); },
      []() { return std::make_unique<arrow::UInt8Scalar>(1); },
      []() { return std::make_unique<arrow::FloatScalar>(1); },
      []() { return std::make_unique<arrow::DoubleScalar>(1); },
  };

  std::uniform_int_distribution<int> dist(0, generators.size() - 1);
  ret.reserve(size);

  for (int i = 0; i < size; ++i) {
    int idx = dist(katana::GetGenerator());
    ret.emplace_back(generators[idx]());
  }

  return ret;
}

struct Visitor : public katana::ArrowVisitor {
  using ResultType = katana::Result<int64_t>;
  using AcceptTypes = std::tuple<katana::AcceptNumericArrowTypes>;

  template <typename ArrowType, typename ScalarType>
  ResultType Call(const ScalarType& scalar) {
    return scalar.value;
  }

  ResultType AcceptFailed(const arrow::Scalar& scalar) {
    return KATANA_ERROR(
        katana::ErrorCode::ArrowError, "no matching type {}",
        scalar.type->name());
  }
};

void
RunVisit(const std::vector<std::unique_ptr<arrow::Scalar>>& scalars) {
  Visitor v;
  size_t total = 0;
  for (const auto& s : scalars) {
    auto res = katana::VisitArrow(v, *s);
    KATANA_LOG_VASSERT(res, "unexpected errror {}", res.error());
    total += res.value();
  }

  KATANA_LOG_VASSERT(
      total == scalars.size(), "{} != {}", total, scalars.size());
}

void
Visit(benchmark::State& state) {
  long size = state.range(0);
  auto input = MakeInput(size);

  for (auto _ : state) {
    RunVisit(input);
  }

  state.SetItemsProcessed(state.iterations() * size);
}

template <class ScalarType>
int64_t
GetValue(ScalarType* scalar) {
  return scalar->value;
}

template <class ScalarType>
katana::Result<int64_t>
GetValueResult(ScalarType* scalar) {
  return scalar->value;
}

void
RunDynamicCast(const std::vector<std::unique_ptr<arrow::Scalar>>& scalars) {
  size_t total = 0;
  for (const auto& s : scalars) {
    if (false) {
      continue;
    }
#define CASE(EnumType, ArrowType)                                              \
  else if (auto* p = dynamic_cast<ArrowType*>(s.get()); p) {                   \
    total += GetValue(p);                                                      \
  }
    CASE(INT8, arrow::Int8Scalar)
    CASE(UINT8, arrow::UInt8Scalar)
    CASE(INT16, arrow::Int16Scalar)
    CASE(UINT16, arrow::UInt16Scalar)
    CASE(INT32, arrow::Int32Scalar)
    CASE(UINT32, arrow::UInt32Scalar)
    CASE(INT64, arrow::Int64Scalar)
    CASE(UINT64, arrow::UInt64Scalar)
    CASE(FLOAT, arrow::FloatScalar)
    CASE(DOUBLE, arrow::DoubleScalar)
#undef CASE
    else {
      continue;
    }
  }

  KATANA_LOG_VASSERT(
      total == scalars.size(), "{} != {}", total, scalars.size());
}

void
DynamicCast(benchmark::State& state) {
  long size = state.range(0);
  auto input = MakeInput(size);

  for (auto _ : state) {
    RunDynamicCast(input);
  }

  state.SetItemsProcessed(state.iterations() * size);
}

void
RunInlineSwitch(const std::vector<std::unique_ptr<arrow::Scalar>>& scalars) {
  size_t total = 0;
  for (const auto& s : scalars) {
    switch (s->type->id()) {
    case arrow::Type::INT8:
    case arrow::Type::UINT8:
    case arrow::Type::INT16:
    case arrow::Type::UINT16:
    case arrow::Type::INT32:
    case arrow::Type::UINT32:
    case arrow::Type::INT64:
    case arrow::Type::UINT64:
    case arrow::Type::FLOAT:
    case arrow::Type::DOUBLE:
      total += 1;
      break;
    default:
      continue;
    }
  }

  KATANA_LOG_VASSERT(
      total == scalars.size(), "{} != {}", total, scalars.size());
}

void
InlineSwitch(benchmark::State& state) {
  long size = state.range(0);
  auto input = MakeInput(size);

  for (auto _ : state) {
    RunInlineSwitch(input);
  }

  state.SetItemsProcessed(state.iterations() * size);
}

void
RunSwitchCast(const std::vector<std::unique_ptr<arrow::Scalar>>& scalars) {
  size_t total = 0;
  for (const auto& s : scalars) {
    switch (s->type->id()) {
#define CASE(EnumType, ArrowType)                                              \
  case arrow::Type::EnumType: {                                                \
    total += GetValue(static_cast<ArrowType*>(s.get()));                       \
    break;                                                                     \
  }
      CASE(INT8, arrow::Int8Scalar)
      CASE(UINT8, arrow::UInt8Scalar)
      CASE(INT16, arrow::Int16Scalar)
      CASE(UINT16, arrow::UInt16Scalar)
      CASE(INT32, arrow::Int32Scalar)
      CASE(UINT32, arrow::UInt32Scalar)
      CASE(INT64, arrow::Int64Scalar)
      CASE(UINT64, arrow::UInt64Scalar)
      CASE(FLOAT, arrow::FloatScalar)
      CASE(DOUBLE, arrow::DoubleScalar)
#undef CASE
    default:
      continue;
    }
  }

  KATANA_LOG_VASSERT(
      total == scalars.size(), "{} != {}", total, scalars.size());
}

void
SwitchCast(benchmark::State& state) {
  long size = state.range(0);
  auto input = MakeInput(size);

  for (auto _ : state) {
    RunSwitchCast(input);
  }

  state.SetItemsProcessed(state.iterations() * size);
}

void
RunSwitchCastResult(
    const std::vector<std::unique_ptr<arrow::Scalar>>& scalars) {
  size_t total = 0;
  for (const auto& s : scalars) {
    switch (s->type->id()) {
#define CASE(EnumType, ArrowType)                                              \
  case arrow::Type::EnumType: {                                                \
    auto res = GetValueResult(static_cast<ArrowType*>(s.get()));               \
    KATANA_LOG_ASSERT(res);                                                    \
    total += res.value();                                                      \
    break;                                                                     \
  }
      CASE(INT8, arrow::Int8Scalar)
      CASE(UINT8, arrow::UInt8Scalar)
      CASE(INT16, arrow::Int16Scalar)
      CASE(UINT16, arrow::UInt16Scalar)
      CASE(INT32, arrow::Int32Scalar)
      CASE(UINT32, arrow::UInt32Scalar)
      CASE(INT64, arrow::Int64Scalar)
      CASE(UINT64, arrow::UInt64Scalar)
      CASE(FLOAT, arrow::FloatScalar)
      CASE(DOUBLE, arrow::DoubleScalar)
#undef CASE
    default:
      continue;
    }
  }

  KATANA_LOG_VASSERT(
      total == scalars.size(), "{} != {}", total, scalars.size());
}

void
SwitchCastResult(benchmark::State& state) {
  long size = state.range(0);
  auto input = MakeInput(size);

  for (auto _ : state) {
    RunSwitchCastResult(input);
  }

  state.SetItemsProcessed(state.iterations() * size);
}

BENCHMARK(InlineSwitch)->Apply(MakeArguments);
BENCHMARK(Visit)->Apply(MakeArguments);
BENCHMARK(DynamicCast)->Apply(MakeArguments);
BENCHMARK(SwitchCast)->Apply(MakeArguments);
BENCHMARK(SwitchCastResult)->Apply(MakeArguments);

}  // namespace

BENCHMARK_MAIN();
