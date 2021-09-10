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

arrow::Type::type
GetArrowTypeID(const arrow::Scalar& scalar) {
  return scalar.type->id();
}

template <typename T>
constexpr decltype(auto)
DispatchCast(const arrow::Scalar& scalar) {
  using CalleeType = const typename arrow::TypeTraits<T>::ScalarType&;
  return static_cast<CalleeType>(scalar);
}

namespace {

void
MakeArguments(benchmark::internal::Benchmark* b) {
  for (long size : {1024, 64 * 1024, 1024 * 1024}) {
    b->Args({size});
  }
}

struct GetValueVisitor {
  using ReturnType = int64_t;
  using ResultType = katana::Result<ReturnType>;

  template <typename ArrowType, typename ScalarType>
  std::enable_if_t<arrow::is_number_type<ArrowType>::value, ResultType> Call(
      const ScalarType& scalar) {
    return scalar.value;
  }

  template <typename... ArrowTypes>
  ResultType Call(...) {
    return KATANA_ERROR(katana::ErrorCode::ArrowError, "no matching call");
  }
};

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

void
RunVisit(const std::vector<std::unique_ptr<arrow::Scalar>>& scalars) {
  GetValueVisitor v;
  size_t total = 0;
  for (const auto& s : scalars) {
    auto res = katana::VisitArrow(*s, v);
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
    if (auto* p = dynamic_cast<arrow::Int8Scalar*>(s.get()); p) {
      total += GetValue(p);
    } else if (auto* p = dynamic_cast<arrow::Int16Scalar*>(s.get()); p) {
      total += GetValue(p);
    } else if (auto* p = dynamic_cast<arrow::Int32Scalar*>(s.get()); p) {
      total += GetValue(p);
    } else if (auto* p = dynamic_cast<arrow::Int64Scalar*>(s.get()); p) {
      total += GetValue(p);
    } else if (auto* p = dynamic_cast<arrow::UInt8Scalar*>(s.get()); p) {
      total += GetValue(p);
    } else if (auto* p = dynamic_cast<arrow::UInt16Scalar*>(s.get()); p) {
      total += GetValue(p);
    } else if (auto* p = dynamic_cast<arrow::UInt32Scalar*>(s.get()); p) {
      total += GetValue(p);
    } else if (auto* p = dynamic_cast<arrow::UInt64Scalar*>(s.get()); p) {
      total += GetValue(p);
    } else if (auto* p = dynamic_cast<arrow::FloatScalar*>(s.get()); p) {
      total += GetValue(p);
    } else if (auto* p = dynamic_cast<arrow::DoubleScalar*>(s.get()); p) {
      total += GetValue(p);
    } else {
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

struct Dispatcher {
  /// CanCall returns true if visitor.Call<ArrowTypes...>(Args...) exists.
  template <typename Visitor, typename... ArrowTypes>
  struct CanCall {
    // Most of the ugliness comes from supporting member functions versus free
    // functions.
    template <typename... Args>
    constexpr static std::enable_if_t<
        std::is_invocable<
            decltype(std::declval<Visitor>().template Call<ArrowTypes...>(
                std::declval<Args>()...)) (Visitor::*)(Args...),
            Visitor&, Args...>::value,
        bool>
    test(void*) {
      return true;
    }

    template <typename...>
    constexpr static bool test(...) {
      return false;
    }

    template <typename... Args>
    constexpr static bool value = test<Args...>(nullptr);
  };

  template <typename V, typename Tuple, size_t... I>
  constexpr static bool TupleContains(std::index_sequence<I...>) {
    return (std::is_same_v<V, std::tuple_element_t<I, Tuple>> || ...);
  }

  // CouldAccept returns true if the ith ArrowType is contained in the ith
  // Visitor::AcceptType tuple.
  template <size_t I, typename Visitor>
  constexpr static bool CouldAccept() {
    return true;
  }

  template <
      size_t I, typename Visitor, typename ArrowType, typename... ArrowTypes>
  constexpr static bool CouldAccept() {
    using Current =
        std::tuple_element_t<I, std::tuple<ArrowType, ArrowTypes...>>;
    using CurrentAcceptTuple =
        std::tuple_element_t<I, typename Visitor::AcceptType>;
    constexpr size_t N = std::tuple_size_v<CurrentAcceptTuple>;
    return TupleContains<Current, CurrentAcceptTuple>(
        std::make_index_sequence<N>());
  }

  template <typename... ArrowTypes, typename Visitor, typename... Args>
  static typename std::decay_t<Visitor>::ReturnType Call(
      Visitor&& visitor, Args&&... args) {
    constexpr auto index = sizeof...(ArrowTypes) - 1;
    if constexpr (!CouldAccept<index, std::decay_t<Visitor>, ArrowTypes...>()) {
      return visitor.AcceptFailed(std::forward<Args>(args)...);
    } else if constexpr (index + 1 == sizeof...(Args)) {
      if constexpr (CanCall<std::decay_t<Visitor>, ArrowTypes...>::
                        template value<Args...>) {
        return visitor.template Call<ArrowTypes...>(
            DispatchCast<ArrowTypes>(std::forward<Args>(args))...);
      } else {
        return visitor.AcceptFailed(std::forward<Args>(args)...);
      }
    } else {
      return Dispatch<ArrowTypes...>(
          std::forward<Visitor>(visitor), std::forward<Args>(args)...);
    }
  }

  template <typename... ArrowTypes, typename Visitor, typename... Args>
  static typename std::decay_t<Visitor>::ReturnType Dispatch(
      Visitor&& visitor, Args&&... args) {
    constexpr auto index = sizeof...(ArrowTypes);
    auto type_id =
        GetArrowTypeID(std::get<index>(std::forward_as_tuple(args...)));

    switch (type_id) {
#define CASE(EnumType)                                                         \
  case arrow::Type::EnumType: {                                                \
    using ArrowType =                                                          \
        typename arrow::TypeIdTraits<arrow::Type::EnumType>::Type;             \
    return Call<ArrowTypes..., ArrowType>(                                     \
        std::forward<Visitor>(visitor), std::forward<Args>(args)...);          \
  }
      CASE(INT8)
      CASE(UINT8)
      CASE(INT16)
      CASE(UINT16)
      CASE(INT32)
      CASE(UINT32)
      CASE(INT64)
      CASE(UINT64)
      CASE(FLOAT)
      CASE(DOUBLE)
#undef CASE
    default:
      return visitor.AcceptFailed(std::forward<Args>(args)...);
    }
  }
};

using NumericLike = std::tuple<
    arrow::Int8Type, arrow::UInt8Type, arrow::Int16Type, arrow::UInt16Type,
    arrow::Int32Type, arrow::UInt32Type, arrow::Int64Type, arrow::UInt64Type,
    arrow::FloatType, arrow::DoubleType>;

static_assert(Dispatcher::TupleContains<arrow::Int8Type, NumericLike>(
    std::make_index_sequence<std::tuple_size_v<NumericLike>>()));

struct Visitor {
  using ReturnType = katana::Result<int64_t>;
  using AcceptType = std::tuple<NumericLike>;

  template <typename... ArrowTypes, typename ScalarType>
  ReturnType Call(const ScalarType& scalar) {
    return scalar.value;
  }

  ReturnType AcceptFailed(const arrow::Scalar& scalar) {
    std::stringstream ss;

    return KATANA_ERROR(
        katana::ErrorCode::ArrowError, "no matching type {}",
        scalar.type->name());
  }
};

static_assert(Dispatcher::CanCall<Visitor, arrow::UInt8Type>::value<
              const arrow::UInt8Scalar&>);

void
RunDispatch(const std::vector<std::unique_ptr<arrow::Scalar>>& scalars) {
  size_t total = 0;
  Visitor visitor;
  for (const auto& s : scalars) {
    auto res = Dispatcher::Call(visitor, *s.get());
    KATANA_LOG_VASSERT(res, "{}", res.error());
    total += res.value();
  }

  KATANA_LOG_VASSERT(
      total == scalars.size(), "{} != {}", total, scalars.size());
}

void
Dispatch(benchmark::State& state) {
  long size = state.range(0);
  auto input = MakeInput(size);

  for (auto _ : state) {
    RunDispatch(input);
  }

  state.SetItemsProcessed(state.iterations() * size);
}

BENCHMARK(InlineSwitch)->Apply(MakeArguments);
BENCHMARK(Visit)->Apply(MakeArguments);
BENCHMARK(DynamicCast)->Apply(MakeArguments);
BENCHMARK(SwitchCast)->Apply(MakeArguments);
BENCHMARK(SwitchCastResult)->Apply(MakeArguments);
BENCHMARK(Dispatch)->Apply(MakeArguments);

}  // namespace

BENCHMARK_MAIN();
