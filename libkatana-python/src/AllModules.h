#ifndef KATANA_LIBKATANAPYTHON_ALLMODULES_H_
#define KATANA_LIBKATANAPYTHON_ALLMODULES_H_

#include <pybind11/pybind11.h>

void InitEntityTypeManager(pybind11::module_& m);
void InitNUMAArray(pybind11::module_& m);
void InitReductions(pybind11::module_& m);

#endif
