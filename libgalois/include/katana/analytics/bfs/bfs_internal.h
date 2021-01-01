#ifndef KATANA_LIBGALOIS_KATANA_ANALYTICS_BFS_BFSINTERNAL_H_
#define KATANA_LIBGALOIS_KATANA_ANALYTICS_BFS_BFSINTERNAL_H_

#include "bfs.h"
#include "katana/analytics/BfsSsspImplementationBase.h"

namespace katana::analytics {

struct BfsImplementation
    : BfsSsspImplementationBase<
          PropertyGraph<std::tuple<BfsNodeDistance>, std::tuple<>>,
          unsigned int, false> {
  BfsImplementation(ptrdiff_t edge_tile_size)
      : BfsSsspImplementationBase<
            PropertyGraph<std::tuple<BfsNodeDistance>, std::tuple<>>,
            unsigned int, false>{edge_tile_size} {}
};

}  // namespace katana::analytics

#endif
