#include <pybind11/pybind11.h>

#include "katana/GraphML.h"
#include "katana/python/CythonIntegration.h"
#include "katana/python/ErrorHandling.h"
#include "katana/python/PythonModuleInitializers.h"

namespace py = pybind11;

void
katana::python::InitImportData(py::module& m) {
  m.def(
      "from_graphml_native",
      [](const std::string& path, uint64_t chunk_size,
         CythonReference<TxnContext> txn_ctx) -> Result<py::object> {
        std::unique_ptr<PropertyGraph> pg;
        {
          py::gil_scoped_release release;
          pg = KATANA_CHECKED(ConvertToPropertyGraph(
              KATANA_CHECKED(ConvertGraphML(path, chunk_size, false)),
              txn_ctx.get()));
        }
        return MakeCythonWrapper(std::shared_ptr<PropertyGraph>(std::move(pg)));
      });
}
