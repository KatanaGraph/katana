#ifndef KATANA_LIBKATANAPYTHONNATIVE_KATANA_PYTHON_PYTHONMODULEINITIALIZERS_H_
#define KATANA_LIBKATANAPYTHONNATIVE_KATANA_PYTHON_PYTHONMODULEINITIALIZERS_H_

#include <pybind11/pybind11.h>

#include "katana/config.h"

namespace katana::python {

KATANA_EXPORT void InitReductions(pybind11::module_& m);

}

#endif
