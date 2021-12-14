#include "katana/Logging.h"
#include "katana/PropertyGraph.h"
#include "katana/SharedMemSys.h"

void
TestEdgeSource(const katana::GraphTopology& topo) noexcept {
  for (auto node : topo.all_nodes()) {
    for (auto e : topo.edges(node)) {
      auto s = topo.edge_source(e);
      KATANA_LOG_ASSERT(s == node);
    }
  }
}

int
main() {
  katana::SharedMemSys S;

  constexpr size_t kNumNodes = 1000;
  constexpr size_t kEdgesPerNode = 5;

  katana::GraphTopology topo =
      katana::CreateUniformRandomTopology(kNumNodes, kEdgesPerNode);

  TestEdgeSource(topo);

  return 0;
}
