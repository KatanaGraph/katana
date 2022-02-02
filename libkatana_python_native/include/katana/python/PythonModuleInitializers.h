#ifndef KATANA_LIBKATANAPYTHONNATIVE_KATANA_PYTHON_PYTHONMODULEINITIALIZERS_H_
#define KATANA_LIBKATANAPYTHONNATIVE_KATANA_PYTHON_PYTHONMODULEINITIALIZERS_H_

#include <pybind11/pybind11.h>

#include "katana/config.h"

namespace katana::python {

KATANA_EXPORT void InitReductions(pybind11::module& m);
KATANA_EXPORT void InitEntityTypeManager(pybind11::module& m);
KATANA_EXPORT void InitImportData(pybind11::module& m);
KATANA_EXPORT void InitPropertyGraph(pybind11::module& m);

}  // namespace katana::python

#endif
