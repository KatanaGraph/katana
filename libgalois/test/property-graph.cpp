#include "TestPropertyGraph.h"

#include <arrow/api.h>
#include <arrow/type.h>
#include <arrow/type_traits.h>

#include "galois/Logging.h"
#include "galois/Properties.h"

namespace gg = galois::graphs;

using DataType = int64_t;

struct Field0 {
  using ViewType  = galois::PODPropertyView<int64_t>;
  using ArrowType = arrow::CTypeTraits<int64_t>::ArrowType;
};

struct Field1 {
  using ViewType  = galois::PODPropertyView<int64_t>;
  using ArrowType = arrow::CTypeTraits<int64_t>::ArrowType;
};

struct Field2 {
  using ViewType  = galois::PODPropertyView<int64_t>;
  using ArrowType = arrow::CTypeTraits<int64_t>::ArrowType;
};

void TestIterate1(size_t num_nodes, size_t line_width) {
  using NodeType = std::tuple<Field0>;
  using EdgeType = std::tuple<Field0>;

  constexpr size_t num_properties = std::tuple_size_v<NodeType>;

  LinePolicy policy{line_width};

  std::unique_ptr<gg::PropertyFileGraph> g =
      MakeFileGraph<DataType>(num_nodes, num_properties, &policy);

  auto r = gg::PropertyGraph<NodeType, EdgeType>::Make(g.get());
  if (!r) {
    GALOIS_LOG_FATAL("could not make property graph: {}", r.error());
  }
  size_t r_baseline = BaselineIterate<Field0, Field0>(g.get(), num_properties);

  size_t r_iterate = Iterate(r.value(), num_properties);

  size_t expected =
      ExpectedValue(g->topology().num_nodes(), g->topology().num_edges(),
                    num_properties, false);

  GALOIS_LOG_VASSERT(r_baseline == r_iterate, "{} != {}", r_baseline,
                     r_iterate);
  GALOIS_LOG_VASSERT(expected == r_iterate, "{} != {}", expected, r_iterate);
}

void TestIterate3(size_t num_nodes, size_t line_width) {
  using NodeType = std::tuple<Field0, Field1, Field2>;
  using EdgeType = std::tuple<Field0, Field1, Field2>;

  constexpr size_t num_properties = std::tuple_size_v<NodeType>;

  LinePolicy policy{line_width};

  std::unique_ptr<gg::PropertyFileGraph> g =
      MakeFileGraph<DataType>(num_nodes, num_properties, &policy);

  auto r = gg::PropertyGraph<NodeType, EdgeType>::Make(g.get());
  if (!r) {
    GALOIS_LOG_FATAL("could not make property graph: {}", r.error());
  }

  size_t r_iterate = Iterate(r.value(), num_properties);
  size_t expected =
      ExpectedValue(g->topology().num_nodes(), g->topology().num_edges(),
                    num_properties, false);
  GALOIS_LOG_VASSERT(expected == r_iterate, "{} != {}", expected, r_iterate);
}

int main() {
  TestIterate1(10, 3);
  TestIterate3(10, 3);

  return 0;
}
