#ifndef KATANA_PYTHON_KATANA_LOCALNATIVE_NUMPY_H_
#define KATANA_PYTHON_KATANA_LOCALNATIVE_NUMPY_H_

#include <pybind11/pybind11.h>

#include "TypeTraits.h"

namespace katana {

template <typename T>
pybind11::object
AsNumpy(
    T* data, size_t size,
    pybind11::object dtype = PythonTypeTraits<T>::default_dtype(),
    bool readonly = false) {
  auto numpy = pybind11::module_::import("numpy");
  auto mem = pybind11::memoryview::from_buffer(
      data, {size}, {sizeof(data[0])}, readonly);
  return numpy.attr("frombuffer")(mem, pybind11::arg("dtype") = dtype);
}

template <typename T>
pybind11::object
AsNumpy(
    const T* data, size_t size,
    pybind11::object dtype = PythonTypeTraits<T>::default_dtype()) {
  return AsNumpy(const_cast<T*>(data), size, dtype, true);
}

template <typename T>
pybind11::object
AsNumpy(
    T& data, pybind11::object dtype = PythonTypeTraits<T>::default_dtype()) {
  return AsNumpy(&data, 1, dtype, false);
}

template <typename T>
pybind11::object
AsNumpy(
    const T& data,
    pybind11::object dtype = PythonTypeTraits<T>::default_dtype()) {
  return AsNumpy(const_cast<T*>(&data), 1, dtype, true);
}

}  // namespace katana

#endif
