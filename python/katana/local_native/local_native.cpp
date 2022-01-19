#include <katana/python/PythonModuleInitializers.h>
#include <pybind11/pybind11.h>

PYBIND11_MODULE(local_native, m) {
  katana::python::InitReductions(m);
  katana::python::InitEntityTypeManager(m);
  katana::python::InitImportData(m);
}
