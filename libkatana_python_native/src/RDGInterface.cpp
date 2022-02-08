#include <pybind11/pybind11.h>

#include "katana/python/CythonIntegration.h"
#include "katana/python/ErrorHandling.h"
#include "katana/python/PythonModuleInitializers.h"
#include "katana/tsuba.h"

namespace py = pybind11;

// NB: This interface is only needed for the out-of-core import code path
// This should NOT be used by anyone else for any kind of purpose since it
// exposes low level details that users do not need to be concerned with.
void
katana::python::InitRDGInterface(py::module& m) {
  // Define the wrapped interface for PropStorageInfo - needed for RDGPartHeader
  // Only need the initial constructor since properites will be in memory
  py::class_<katana::RDGPropInfo> rdg_prop_info_cls(m, "RDGPropInfo");
  rdg_prop_info_cls.def(py::init([](std::string name, std::string path) {
    return katana::RDGPropInfo{name, path};
  }));

  m.def("write_rdg_part_header", &katana::WriteRDGPartHeader);
}
