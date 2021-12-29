#ifndef KATANA_PYTHON_KATANA_LOCALNATIVE_TEMPLATESUPPORT_H_
#define KATANA_PYTHON_KATANA_LOCALNATIVE_TEMPLATESUPPORT_H_

#include <pybind11/pybind11.h>

#include "TypeTraits.h"

namespace katana {

// TODO(amp): Add support for numpy structs
template <int n>
struct OpaqueValue {
  char data[n];
};

template <typename T, typename F>
pybind11::object
InstantiateForType(
    pybind11::module_ m, const std::string& basename, F f,
    pybind11::dict types) {
  pybind11::object dtype = katana::PythonTypeTraits<T>::default_dtype();
  pybind11::object cls = f.template operator()<T>(
      m, (basename + "[" + katana::PythonTypeTraits<T>::name + "]").c_str());
  types[dtype] = cls;
  return cls;
}

template <typename F>
void
InstantiateForStandardTypes(
    pybind11::module_ m, const std::string& basename, F f) {
  pybind11::module_ builtins = pybind11::module_::import("builtins");
  pybind11::dict types;
  InstantiateForType<uint8_t>(m, basename, f, types);
  InstantiateForType<uint16_t>(m, basename, f, types);
  InstantiateForType<uint32_t>(m, basename, f, types);
  InstantiateForType<uint64_t>(m, basename, f, types);
  InstantiateForType<int8_t>(m, basename, f, types);
  InstantiateForType<int16_t>(m, basename, f, types);
  InstantiateForType<int32_t>(m, basename, f, types);
  // Set the builtin type int as an alias for int64
  types[builtins.attr("int")] =
      InstantiateForType<int64_t>(m, basename, f, types);
  InstantiateForType<float>(m, basename, f, types);
  // Set the builtin type float as an alias for float64/double
  types[builtins.attr("float")] =
      InstantiateForType<double>(m, basename, f, types);
  m.attr(basename.c_str()) = types;
}

}  // namespace katana

#endif
