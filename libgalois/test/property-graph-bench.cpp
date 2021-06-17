#include <benchmark/benchmark.h>

#include "TestTypedPropertyGraph.h"
#include "katana/Logging.h"
#include "katana/PropertyGraph.h"
#include "katana/TypedPropertyGraph.h"

using DataType = int64_t;

#define FIELD(Number)                                                          \
  struct Field##Number {                                                       \
    using ViewType = katana::PODPropertyView<DataType>;                        \
    using ArrowType = arrow::CTypeTraits<DataType>::ArrowType;                 \
  }

FIELD(0);
FIELD(1);
FIELD(2);
FIELD(3);
FIELD(4);
FIELD(5);
FIELD(6);
FIELD(7);
FIELD(8);
FIELD(9);

#undef FIELD

namespace {

void
MakeArguments(benchmark::internal::Benchmark* b) {
  for (int i = 0; i < 3; ++i) {
    for (int j = 0; j < 2; ++j) {
      long num_nodes = 1 << (i * 8 + 10);
      long num_properties = j * 3 + 1;
      b->Args({num_nodes, num_properties});
    }
  }
}

template <size_t>
struct PropertyTuple {};

template <>
struct PropertyTuple<1> {
  using type = std::tuple<Field0>;
};

template <>
struct PropertyTuple<4> {
  using type = std::tuple<Field0, Field1, Field2, Field3>;
};

template <>
struct PropertyTuple<7> {
  using type =
      std::tuple<Field0, Field1, Field2, Field3, Field4, Field5, Field6>;
};

template <>
struct PropertyTuple<10> {
  using type = std::tuple<
      Field0, Field1, Field2, Field3, Field4, Field5, Field6, Field7, Field8,
      Field9>;
};

template <size_t num_properties>
void
IterateProperty(benchmark::State& state, katana::PropertyGraph* g) {
  using P = typename PropertyTuple<num_properties>::type;

  auto r = katana::TypedPropertyGraph<P, P>::Make(g);
  if (!r) {
    KATANA_LOG_FATAL("could not make property graph: {}", r.error());
  }

  for (auto _ : state) {
    size_t r_iterate = Iterate(r.value(), num_properties);
    size_t expected = ExpectedValue(
        g->topology().num_nodes(), g->topology().num_edges(), num_properties,
        false);
    KATANA_LOG_VASSERT(
        r_iterate == expected, "expected {} found {}", expected, r_iterate);
  }
}

void
IterateProperty(benchmark::State& state) {
  auto [num_nodes, num_properties] =
      std::make_tuple(state.range(0), state.range(1));

  RandomPolicy policy{4};

  std::unique_ptr<katana::PropertyGraph> g =
      MakeFileGraph<DataType>(num_nodes, num_properties, &policy);

  switch (num_properties) {
  case 1:
    return IterateProperty<1>(state, g.get());
  case 4:
    return IterateProperty<4>(state, g.get());
  case 7:
    return IterateProperty<7>(state, g.get());
  case 10:
    return IterateProperty<10>(state, g.get());
  default:
    KATANA_LOG_FATAL("unexpected number of properties: {}", num_properties);
  }
}

void
IterateBaseline(benchmark::State& state) {
  auto [num_nodes, num_properties] =
      std::make_tuple(state.range(0), state.range(1));

  RandomPolicy policy{4};

  std::unique_ptr<katana::PropertyGraph> g =
      MakeFileGraph<DataType>(num_nodes, num_properties, &policy);

  for (auto _ : state) {
    size_t r = BaselineIterate<Field0, Field0>(g.get(), num_properties);
    size_t expected = ExpectedValue(
        g->topology().num_nodes(), g->topology().num_edges(), num_properties,
        false);
    KATANA_LOG_VASSERT(r == expected, "expected {} found {}", expected, r);
  }
}

BENCHMARK(IterateBaseline)->Apply(MakeArguments);
BENCHMARK(IterateProperty)->Apply(MakeArguments);

}  // namespace

int main(int argc, char** argv) {
  ::benchmark::Initialize(&argc, argv);
  katana::SharedMemSys G;
  ::benchmark::RunSpecifiedBenchmarks();
}
