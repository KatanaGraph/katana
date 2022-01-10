#ifndef KATANA_LIBKATANAPYTHONNATIVE_KATANA_PYTHON_TYPETRAITS_H_
#define KATANA_LIBKATANAPYTHONNATIVE_KATANA_PYTHON_TYPETRAITS_H_

#include <pybind11/pybind11.h>

namespace katana {

template <typename T, typename Enable = void>
struct PythonTypeTraits {};

#define PYTHON_TYPE_TRAITS(T, numpy_name, ctypes_name)                         \
  template <>                                                                  \
  struct PythonTypeTraits<T> {                                                 \
    static constexpr const char* name = numpy_name;                            \
    static pybind11::object default_dtype() {                                  \
      pybind11::module numpy = pybind11::module::import("numpy");              \
      return numpy.attr(numpy_name);                                           \
    }                                                                          \
    static pybind11::object ctypes_type() {                                    \
      pybind11::module ctypes = pybind11::module::import("ctypes");            \
      return ctypes.attr(ctypes_name);                                         \
    }                                                                          \
  }

#define PYTHON_TYPE_TRAITS_BY_PREFIX(prefix)                                   \
  PYTHON_TYPE_TRAITS(prefix##_t, #prefix, "c_" #prefix)

PYTHON_TYPE_TRAITS_BY_PREFIX(uint8);
PYTHON_TYPE_TRAITS_BY_PREFIX(uint16);
PYTHON_TYPE_TRAITS_BY_PREFIX(uint32);
PYTHON_TYPE_TRAITS_BY_PREFIX(uint64);

PYTHON_TYPE_TRAITS_BY_PREFIX(int8);
PYTHON_TYPE_TRAITS_BY_PREFIX(int16);
PYTHON_TYPE_TRAITS_BY_PREFIX(int32);
PYTHON_TYPE_TRAITS_BY_PREFIX(int64);

PYTHON_TYPE_TRAITS(float, "float32", "c_float");
PYTHON_TYPE_TRAITS(double, "float64", "c_double");

#undef PYTHON_TYPE_TRAITS_BY_PREFIX
#undef PYTHON_TYPE_TRAITS

/// There is no numpy dtype for bool. Requesting it will give a C++ compiler
/// error.
template <>
struct PythonTypeTraits<bool> {
  static constexpr const char* name = "bool";
  static pybind11::object ctypes_type() {
    pybind11::module ctypes = pybind11::module::import("ctypes");
    return ctypes.attr("c_bool");
  }
};

template <>
struct PythonTypeTraits<void> {
  static constexpr const char* name = "void";
  static pybind11::object ctypes_type() { return pybind11::none(); }
};

}  // namespace katana

#endif
