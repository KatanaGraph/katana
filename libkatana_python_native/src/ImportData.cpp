#include <pybind11/numpy.h>
#include <pybind11/pybind11.h>

#include "katana/GraphML.h"
#include "katana/ParallelSTL.h"
#include "katana/python/CythonIntegration.h"
#include "katana/python/ErrorHandling.h"
#include "katana/python/PropertyGraphPython.h"
#include "katana/python/PythonModuleInitializers.h"

namespace py = pybind11;

namespace {

katana::GraphTopology
TopologyFromCSR(
    const py::array_t<katana::PropertyGraph::Edge::underlying_type>&
        edge_indices,
    const py::array_t<katana::PropertyGraph::Node::underlying_type>&
        edge_destinations) {
  return katana::GraphTopology(
      edge_indices.data(), edge_indices.size(), edge_destinations.data(),
      edge_destinations.size());
}

}  // namespace

void
katana::python::InitImportData(py::module& m) {
  m.def(
      "from_graphml_native",
      [](const std::string& path, uint64_t chunk_size,
         TxnContext* txn_ctx) -> Result<std::shared_ptr<PropertyGraph>> {
        py::gil_scoped_release release;
        return KATANA_CHECKED(ConvertToPropertyGraph(
            KATANA_CHECKED(ConvertGraphML(path, chunk_size, false)), txn_ctx));
      });

  m.def(
      "from_csr",
      [](py::array_t<PropertyGraph::Edge::underlying_type> edge_indices,
         py::array_t<PropertyGraph::Node::underlying_type> edge_destinations)
          -> Result<std::shared_ptr<PropertyGraph>> {
        return KATANA_CHECKED(katana::PropertyGraph::Make(
            TopologyFromCSR(edge_indices, edge_destinations)));
      },
      R"""(
      Create a new `Graph` from a raw Compressed Sparse Row representation.

      :param edge_indices: The indices of the first edge for each node in the destinations vector.
      :type edge_indices: `numpy.ndarray` or another type supporting the buffer protocol. Element type must be an
          integer.
      :param edge_destinations: The destinations of edges in the new graph.
      :type edge_destinations: `numpy.ndarray` or another type supporting the buffer protocol. Element type must be an
          integer.
      :returns: the new :py:class:`~katana.local.Graph`
      )""");

  m.def(
      "_from_csr_and_raw_types",
      [](const py::array_t<PropertyGraph::Edge::underlying_type> edge_indices,
         const py::array_t<PropertyGraph::Node::underlying_type>
             edge_destinations,
         const py::array_t<EntityTypeID> node_types,
         const py::array_t<EntityTypeID> edge_types,
         const EntityTypeManager& node_type_manager,
         const EntityTypeManager& edge_type_manager)
          -> Result<std::shared_ptr<PropertyGraph>> {
        NUMAArray<EntityTypeID> node_types_owned;
        node_types_owned.allocateBlocked(node_types.size());
        katana::ParallelSTL::copy(
            node_types.data(), node_types.data() + node_types.size(),
            node_types_owned.begin());
        NUMAArray<EntityTypeID> edge_types_owned;
        edge_types_owned.allocateBlocked(edge_types.size());
        katana::ParallelSTL::copy(
            edge_types.data(), edge_types.data() + edge_types.size(),
            edge_types_owned.begin());
        EntityTypeManager node_type_manager_owned = node_type_manager;
        EntityTypeManager edge_type_manager_owned = edge_type_manager;
        return KATANA_CHECKED(katana::PropertyGraph::Make(
            TopologyFromCSR(edge_indices, edge_destinations),
            std::move(node_types_owned), std::move(edge_types_owned),
            std::move(node_type_manager_owned),
            std::move(edge_type_manager_owned)));
      });
}
