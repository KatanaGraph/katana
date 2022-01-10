#include <katana/python/PythonModuleInitializers.h>
#include <pybind11/pybind11.h>

#ifdef PYBIND11_MODULE
// pybind11 >= 2.2
PYBIND11_MODULE(local_native, m) { katana::python::InitReductions(m); }
#else
// pybind11 < 2.2
PYBIND11_PLUGIN(local_native) {
  pybind11::module m("local_native");
  katana::python::InitReductions(m);
  return m.ptr();
}
#endif
