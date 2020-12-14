#ifndef GALOIS_LIBGALOIS_GALOIS_ANALYTICS_BFS_BFSINTERNAL_H_
#define GALOIS_LIBGALOIS_GALOIS_ANALYTICS_BFS_BFSINTERNAL_H_

#include "bfs.h"
#include "galois/analytics/BfsSsspImplementationBase.h"

namespace galois::analytics {

struct BfsImplementation
    : BfsSsspImplementationBase<
          graphs::PropertyGraph<std::tuple<BfsNodeDistance>, std::tuple<>>,
          unsigned int, false> {
  BfsImplementation(ptrdiff_t edge_tile_size)
      : BfsSsspImplementationBase<
            graphs::PropertyGraph<std::tuple<BfsNodeDistance>, std::tuple<>>,
            unsigned int, false>{edge_tile_size} {}
};

}  // namespace galois::analytics

#endif
