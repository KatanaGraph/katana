#include <arrow/api.h>
#include <arrow/type.h>
#include <arrow/type_traits.h>

#include "TestTypedPropertyGraph.h"
#include "katana/Logging.h"
#include "katana/Properties.h"
#include "katana/PropertyIndex.h"

template<typename node_or_edge, typename DataType>
void TestIndex(size_t num_nodes, size_t num_properties, size_t line_width) {

  LinePolicy policy{line_width};

  std::unique_ptr<katana::PropertyGraph> g =
      MakeFileGraph<DataType>(num_nodes, num_properties, &policy);
  
  auto index = katana::PropertyIndex<node_or_edge>::Make(g.get(), "1");
  KATANA_LOG_VASSERT(index, "Could not create index: {}", index.error());

  // KATANA_LOG_VASSERT(
  //     false, "{} {} {}", g->GetNodePropertyName(0),
  //     g->GetNodePropertyName(1),
  //     g->GetNodePropertyName(2)); 
}

int
main() {
  katana::SharedMemSys S;

  TestIndex<katana::GraphTopology::Node, int64_t>(10, 3, 3);
  TestIndex<katana::GraphTopology::Edge, int64_t>(10, 3, 3);

  return 0;
}