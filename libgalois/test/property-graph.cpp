#include <arrow/api.h>
#include <arrow/type.h>
#include <arrow/type_traits.h>

#include "TestTypedPropertyGraph.h"
#include "katana/Logging.h"
#include "katana/Properties.h"

using DataType = int64_t;

struct Field0 {
  using ViewType = katana::PODPropertyView<int64_t>;
  using ArrowType = arrow::CTypeTraits<int64_t>::ArrowType;
};

struct Field1 {
  using ViewType = katana::PODPropertyView<int64_t>;
  using ArrowType = arrow::CTypeTraits<int64_t>::ArrowType;
};

struct Field2 {
  using ViewType = katana::PODPropertyView<int64_t>;
  using ArrowType = arrow::CTypeTraits<int64_t>::ArrowType;
};

void
TestIterate1(size_t num_nodes, size_t line_width) {
  using NodeType = std::tuple<Field0>;
  using EdgeType = std::tuple<Field0>;

  constexpr size_t num_properties = std::tuple_size_v<NodeType>;

  LinePolicy policy{line_width};

  std::unique_ptr<katana::PropertyGraph> g =
      MakeFileGraph<DataType>(num_nodes, num_properties, &policy);

  auto r = katana::TypedPropertyGraph<NodeType, EdgeType>::Make(g.get());
  if (!r) {
    KATANA_LOG_FATAL("could not make property graph: {}", r.error());
  }
  size_t r_baseline = BaselineIterate<Field0, Field0>(g.get(), num_properties);

  size_t r_iterate = Iterate(r.value(), num_properties);

  size_t expected = ExpectedValue(
      g->topology().num_nodes(), g->topology().num_edges(), num_properties,
      false);

  KATANA_LOG_VASSERT(
      r_baseline == r_iterate, "{} != {}", r_baseline, r_iterate);
  KATANA_LOG_VASSERT(expected == r_iterate, "{} != {}", expected, r_iterate);
}

void
TestIterate3(size_t num_nodes, size_t line_width) {
  using NodeType = std::tuple<Field0, Field1, Field2>;
  using EdgeType = std::tuple<Field0, Field1, Field2>;

  constexpr size_t num_properties = std::tuple_size_v<NodeType>;

  LinePolicy policy{line_width};

  std::unique_ptr<katana::PropertyGraph> g =
      MakeFileGraph<DataType>(num_nodes, num_properties, &policy);

  auto r = katana::TypedPropertyGraph<NodeType, EdgeType>::Make(g.get());
  if (!r) {
    KATANA_LOG_FATAL("could not make property graph: {}", r.error());
  }

  size_t r_iterate = Iterate(r.value(), num_properties);
  size_t expected = ExpectedValue(
      g->topology().num_nodes(), g->topology().num_edges(), num_properties,
      false);
  KATANA_LOG_VASSERT(expected == r_iterate, "{} != {}", expected, r_iterate);
}

/// Test using only part off a PropertyGraph
void
TestIterate4(size_t num_nodes, size_t line_width) {
  using NodeType = std::tuple<Field0, Field1>;
  using EdgeType = std::tuple<Field0, Field1>;

  constexpr size_t num_properties = std::tuple_size_v<NodeType>;

  LinePolicy policy{line_width};

  std::unique_ptr<katana::PropertyGraph> g =
      MakeFileGraph<DataType>(num_nodes, 5, &policy);

  auto r = katana::TypedPropertyGraph<NodeType, EdgeType>::Make(
      g.get(), {"1", "3"}, {"0", "4"});
  if (!r) {
    KATANA_LOG_FATAL("could not make property graph: {}", r.error());
  }

  size_t r_iterate = Iterate(r.value(), num_properties);
  size_t expected = ExpectedValue(
      g->topology().num_nodes(), g->topology().num_edges(), num_properties,
      false);
  KATANA_LOG_VASSERT(expected == r_iterate, "{} != {}", expected, r_iterate);
}

/// Test non-existent property error
void
TestError1(size_t num_nodes, size_t line_width) {
  using NodeType = std::tuple<Field0, Field1>;
  using EdgeType = std::tuple<Field0, Field1>;

  LinePolicy policy{line_width};

  std::unique_ptr<katana::PropertyGraph> g =
      MakeFileGraph<DataType>(num_nodes, 5, &policy);

  auto r1 = katana::TypedPropertyGraph<NodeType, EdgeType>::Make(
      g.get(), {"1", "3"}, {"0", "noexist"});
  KATANA_LOG_VASSERT(
      r1.error() == katana::ErrorCode::PropertyNotFound,
      "Should return PropertyNotFound when edge property doesn't exist.");

  auto r2 = katana::TypedPropertyGraph<NodeType, EdgeType>::Make(
      g.get(), {"noexist", "3"}, {"0", "2"});
  KATANA_LOG_VASSERT(
      r2.error() == katana::ErrorCode::PropertyNotFound,
      "Should return PropertyNotFound when node property doesn't exist.");
}

int
main() {
  katana::SharedMemSys S;

  TestIterate1(10, 3);
  TestIterate3(10, 3);
  TestIterate4(10, 3);
  TestError1(10, 3);

  return 0;
}
