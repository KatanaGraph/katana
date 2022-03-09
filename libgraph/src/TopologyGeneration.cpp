#include "katana/TopologyGeneration.h"

namespace {
template <typename F>
std::unique_ptr<katana::PropertyGraph>
MakeTopologyImpl(F builder_fun) {
  katana::SymmetricGraphTopologyBuilder builder;
  builder_fun(builder);

  katana::GraphTopology topo = builder.ConvertToCSR();
  auto res = katana::PropertyGraph::Make(std::move(topo));
  KATANA_LOG_ASSERT(res);
  return std::move(res.value());
}
}  // namespace

namespace katana {

std::unique_ptr<katana::PropertyGraph>
MakeGrid(size_t width, size_t height, bool with_diagonals) noexcept {
  /*
  This generator builds a graph with the regular grid topology, where each cell
  of the grid looks like so:

    i===i+1
    ║\ /|
    ║ \ |
    ║/ \|
  i+N---i+N+1

  The diagonals and the double-lines above are the edges we explicitly add for
  every cell. Right and bottom boundary cells only add the double-line edges.
  */

  size_t total_nodes = width * height;

  return MakeTopologyImpl([&](auto& builder) {
    builder.AddNodes(total_nodes);

    // Iterate over every grid cell.
    for (size_t n = 0; n < total_nodes - 1; ++n) {
      if ((n + 1) % width == 0) {
        // this node is at the right boundary
        builder.AddEdge(n, n + width);
      } else if (n >= total_nodes - width) {
        // this node is at the bottom boundary
        builder.AddEdge(n, n + 1);
      } else {
        // Horizontal edge
        builder.AddEdge(n, n + 1);
        // Vertical edge
        builder.AddEdge(n, n + width);

        // Diagonals
        if (with_diagonals) {
          builder.AddEdge(n, n + width + 1);
          builder.AddEdge(n + 1, n + width);
        }
      }
    }
  });
}

std::unique_ptr<katana::PropertyGraph>
MakeFerrisWheel(size_t num_nodes) noexcept {
  /*
          * * 3 * *
        * *   *   * *
      2       *       4
    *   *     *     *   *
  * *     *   *   *     * *
  *         * * *         *
  1 * * * * * 0 * * * * * 5    Topology for num_nodes = 9
  *         * * *         *
  * *     *   *   *     * *
    *   *     *     *   *
      8       *       6
        * *   *   * *
          * * 7 * *
  */

  KATANA_LOG_ASSERT(num_nodes > 4);

  return MakeTopologyImpl([&](auto& builder) {
    builder.AddNodes(num_nodes);

    for (size_t n = 1; n < num_nodes; ++n) {
      // Spock
      builder.AddEdge(n, 0);

      // Next neighbor
      size_t next = (n < num_nodes - 1) ? (n + 1) : 1;
      builder.AddEdge(n, next);
    }
  });
}

KATANA_EXPORT std::unique_ptr<katana::PropertyGraph>
MakeSawtooth(size_t length) noexcept {
  /*
        1     3     5
       / \   / \   / \    Topology for length = 3
      /   \ /   \ /   \
     0-----2-----4-----6
  */

  return MakeTopologyImpl([&](auto& builder) {
    builder.AddNodes(2 * length + 1);

    // Tooth sides
    for (size_t n = 1; n < 2 * length; n += 2) {
      builder.AddEdge(n, n - 1);
      builder.AddEdge(n, n + 1);
    }

    // Tooth base
    for (size_t n = 0; n < 2 * length - 1; n += 2) {
      builder.AddEdge(n, n + 2);
    }
  });
}

KATANA_EXPORT std::unique_ptr<katana::PropertyGraph>
MakeClique(size_t num_nodes) noexcept {
  KATANA_LOG_ASSERT(num_nodes > 2);

  return MakeTopologyImpl([&](auto& builder) {
    // Every node is connected to every other node.
    builder.AddNodes(num_nodes);

    for (size_t n = 0; n < num_nodes; ++n) {
      for (size_t m = n + 1; m < num_nodes; ++m) {
        builder.AddEdge(n, m);
      }
    }
  });
}

KATANA_EXPORT std::unique_ptr<katana::PropertyGraph>
MakeTriangle(size_t num_rows) noexcept {
  /*
  Here, num_rows is the number of rows of triangles, not the rows of nodes.
  E.g. a single-row topology has exactly one triangle.

           0
          / \
         /   \
        1-----2     Topology for num_rows = 2
       / \   / \
      /   \ /   \
     3-----4-----5
  */

  KATANA_LOG_ASSERT(num_rows > 0);

  size_t total_nodes = (num_rows + 1) * (num_rows + 2) / 2;

  return MakeTopologyImpl([&](auto& builder) {
    builder.AddNodes(total_nodes);

    size_t starting_idx = 0;
    size_t num_nodes = 1;
    for (size_t i = 0; i < num_rows; ++i) {
      const size_t ending_idx = starting_idx + num_nodes;
      for (size_t n = starting_idx; n < ending_idx; ++n) {
        // Left and right sides
        builder.AddEdge(n, n + num_nodes);
        builder.AddEdge(n, n + num_nodes + 1);

        // Bottom side
        builder.AddEdge(n + num_nodes, n + num_nodes + 1);
      }
      starting_idx = ending_idx;
      num_nodes += 1;
    }
  });
}

}  // namespace katana
