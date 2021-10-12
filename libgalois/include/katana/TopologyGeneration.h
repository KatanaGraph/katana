#ifndef KATANA_LIBGALOIS_KATANA_TOPOLOGYGENERATION_H_
#define KATANA_LIBGALOIS_KATANA_TOPOLOGYGENERATION_H_

#include "katana/PropertyGraph.h"

namespace katana {
/// Generates a graph with the topology of a regular N x N grid, with diagonals
/// in every cell.
KATANA_EXPORT std::unique_ptr<katana::PropertyGraph> MakeGrid(
    size_t width, size_t height, bool with_diagonals) noexcept;

/// Generates a graph with the Ferris wheel topology: N - 1 nodes on the circle,
/// each connected to 2 neighbors on the circle and 1 central node.
KATANA_EXPORT std::unique_ptr<katana::PropertyGraph> MakeFerrisWheel(
    size_t num_nodes) noexcept;

/// Generates a graph with the sawtooth topology. Nodes are arranged into two rows.
/// First row has N nodes, second row has N+1 nodes. We connect ith-node in first row
/// with (ith and i+1th) nodes in second row.
KATANA_EXPORT std::unique_ptr<katana::PropertyGraph> MakeSawtooth(
    size_t length) noexcept;

/// Generates an N-clique.
KATANA_EXPORT std::unique_ptr<katana::PropertyGraph> MakeClique(
    size_t num_nodes) noexcept;

/// Generates an graph with the triangular array topology.
KATANA_EXPORT std::unique_ptr<katana::PropertyGraph> MakeTriangle(
    size_t num_rows) noexcept;
}  // end namespace katana

#endif  // KATANA_LIBGALOIS_KATANA_TOPOLOGYGENERATION_H_
