#ifndef KATANA_LIBKATANAPYTHONNATIVE_KATANA_PYTHON_TEMPLATESUPPORT_H_
#define KATANA_LIBKATANAPYTHONNATIVE_KATANA_PYTHON_TEMPLATESUPPORT_H_

#include <katana/python/TypeTraits.h>
#include <pybind11/pybind11.h>

namespace katana {

// TODO(amp): Use template template parameters once we can rely on having them.

template <typename T>
std::string
GetInstantiationName(const std::string& basename) {
  return basename + "[" + PythonTypeTraits<T>::name + "]";
}

/// Invoke @c f&lt;T&gt;(m, name) and assign the resulting class into
/// @c types[T].
///
/// \param m The module which should contain the resulting class.
/// \param basename The base name of the class which will be suffixed with a type name.
/// \param f The functor which instantiates the class. It must provide a method
///     @c instantiate&lt;T&gt; which instantiates and wraps the type at @c T.
/// \param types A dict to fill with instantiations.
/// \return The resulting class.
template <typename T, typename F>
pybind11::object
InstantiateForType(
    pybind11::module m, const std::string& basename, F f,
    pybind11::dict types) {
  pybind11::object representation =
      katana::PythonTypeTraits<T>::representation();
  pybind11::object cls =
      f.template instantiate<T>(m, GetInstantiationName<T>(basename).c_str());
  types[representation] = cls;
  return cls;
}

template <typename... Types, typename F>
void
InstantiateForTypes(pybind11::module m, const std::string& basename, F f) {
  pybind11::module builtins = pybind11::module::import("builtins");
  auto make_template_type1 = pybind11::cast<pybind11::function>(
      pybind11::module::import("katana.native_interfacing.template_type")
          .attr("make_template_type1"));
  pybind11::dict types;

  // Parameter pack expansion is only allowed in the arguments or initializer
  // for something. The array is unused and only exists to allow that pack
  // expansion.
  pybind11::object classes_unused[] = {
      InstantiateForType<Types>(m, basename, f, types)...};

  // Set the builtin type int as an alias for int64 if int64 exists.
  if (types.template contains(
          katana::PythonTypeTraits<int64_t>::representation())) {
    types[builtins.attr("int")] =
        types[katana::PythonTypeTraits<int64_t>::representation()];
  }
  // Set the builtin type float as an alias for float64/double if double exists.
  if (types.template contains(
          katana::PythonTypeTraits<double>::representation())) {
    types[builtins.attr("float")] =
        types[katana::PythonTypeTraits<double>::representation()];
  }

  m.attr(basename.c_str()) = make_template_type1(basename, types);
}

/// Invoke `f<T>(m, fullname)` for a set of standard types and store the
/// resulting collection of classes in `m.basename` for use from Python.
///
/// \param m The module which should contain the resulting class.
/// \param basename The base name of the class which will be suffixed with a
///     type name.
/// \param f The functor which instantiates the class. It must provide a method
///     @c instantiate&lt;T&gt; which instantiates and wraps the type at @c T.
template <typename... Types, typename F>
void
InstantiateForStandardTypes(
    pybind11::module m, const std::string& basename, F f) {
  InstantiateForTypes<
      uint8_t, uint16_t, uint32_t, uint64_t, int8_t, int16_t, int32_t, int64_t,
      float, double>(m, basename, f);
}

}  // namespace katana

#endif
