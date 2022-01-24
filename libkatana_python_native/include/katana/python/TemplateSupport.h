#ifndef KATANA_LIBKATANAPYTHONNATIVE_KATANA_PYTHON_TEMPLATESUPPORT_H_
#define KATANA_LIBKATANAPYTHONNATIVE_KATANA_PYTHON_TEMPLATESUPPORT_H_

#include <katana/python/TypeTraits.h>
#include <pybind11/pybind11.h>

namespace katana {

// TODO(amp): Use template template parameters once we can rely on having them.

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
  pybind11::object dtype = katana::PythonTypeTraits<T>::default_dtype();
  pybind11::object cls = f.template instantiate<T>(
      m, (basename + "[" + katana::PythonTypeTraits<T>::name + "]").c_str());
  types[dtype] = cls;
  return cls;
}

/// Invoke `f<T>(m, fullname)` for a set of standard types and store the
/// resulting collection of classes in `m.basename` for use from Python.
///
/// \param m The module which should contain the resulting class.
/// \param basename The base name of the class which will be suffixed with a
///     type name.
/// \param f The functor which instantiates the class. It must provide a method
///     @c instantiate&lt;T&gt; which instantiates and wraps the type at @c T.
template <typename F>
void
InstantiateForStandardTypes(
    pybind11::module m, const std::string& basename, F f) {
  pybind11::module builtins = pybind11::module::import("builtins");
  auto make_template_type1 = pybind11::cast<pybind11::function>(
      pybind11::module::import("katana.native_interfacing.template_type")
          .attr("make_template_type1"));
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
  m.attr(basename.c_str()) = make_template_type1(basename, types);
}

}  // namespace katana

#endif
