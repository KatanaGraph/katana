#include <katana/SharedMemSys.h>
#include <pybind11/pybind11.h>

#include "AllModules.h"

namespace py = pybind11;

PYBIND11_MODULE(local_native, m) { InitReductions(m); }
