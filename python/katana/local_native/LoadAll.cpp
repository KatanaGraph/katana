#include <katana/SharedMemSys.h>
#include <pybind11/pybind11.h>

#include "AllModules.h"

namespace py = pybind11;

PYBIND11_MODULE(local_native, m) {
  py::class_<katana::SharedMemSys>(m, "SharedMemSys").def(py::init());

  InitEntityTypeManager(m);
  InitNUMAArray(m);
  InitReductions(m);
}
