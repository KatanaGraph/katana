#include <pybind11/numpy.h>
#include <pybind11/pybind11.h>

#include "katana/GraphML.h"
#include "katana/python/CythonIntegration.h"
#include "katana/python/ErrorHandling.h"
#include "katana/python/PropertyGraphPython.h"
#include "katana/python/PythonModuleInitializers.h"

namespace py = pybind11;

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
      [](py::array_t<PropertyGraph::Edge> edge_indices,
         py::array_t<PropertyGraph::Node> edge_destinations)
          -> Result<std::shared_ptr<PropertyGraph>> {
        return KATANA_CHECKED(katana::PropertyGraph::Make(GraphTopology(
            edge_indices.data(), edge_indices.size(), edge_destinations.data(),
            edge_destinations.size())));
      },
      R"""(
      Create a new `Graph` from a raw Compressed Sparse Row representation.

      :param edge_indices: The indicies of the first edge for each node in the destinations vector.
      :type edge_indices: `numpy.ndarray` or another type supporting the buffer protocol. Element type must be an
          integer.
      :param edge_destinations: The destinations of edges in the new graph.
      :type edge_destinations: `numpy.ndarray` or another type supporting the buffer protocol. Element type must be an
          integer.
      :returns: the new :py:class:`~katana.local.Graph`
      )""");
}
