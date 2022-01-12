#ifndef KATANA_LIBKATANAPYTHONNATIVE_KATANA_PYTHON_CYTHONINTEGRATION_H_
#define KATANA_LIBKATANAPYTHONNATIVE_KATANA_PYTHON_CYTHONINTEGRATION_H_

#include <katana/Bag.h>
#include <katana/PropertyGraph.h>
#include <pybind11/pybind11.h>

namespace katana {

template <typename T>
struct CythonReferenceSupported : std::false_type {};

template <
    typename T,
    std::enable_if_t<CythonReferenceSupported<T>::value, bool> = true>
class CythonReference {
  pybind11::object wrapper_;
  T* ptr_ = 0;

public:
  CythonReference(pybind11::handle wrapper) {
    wrapper_ = pybind11::reinterpret_borrow<pybind11::object>(wrapper);
    ptr_ = reinterpret_cast<T*>(
        pybind11::cast<uintptr_t>(wrapper.attr("__katana_address__")));
  }

  CythonReference() = default;
  CythonReference(const CythonReference&) = default;
  CythonReference(CythonReference&&) = default;

  CythonReference& operator=(const CythonReference&) = default;
  CythonReference& operator=(CythonReference&&) = default;

  T* operator->() { return ptr_; }
  const T* operator->() const { return ptr_; }

  T* get() { return ptr_; }
  const T* get() const { return ptr_; }

  pybind11::object wrapper() const { return wrapper_; }

  static pybind11::object python_class() {
    return CythonReferenceSupported<T>::python_class();
  }
};

}  // namespace katana

namespace pybind11 {
namespace detail {
/// Automatic cast from/to Python for Cython types which support it.
// std::enable_if_t<std::is_integral<Integer>::value, bool> = true
template <typename T>
struct type_caster<katana::CythonReference<T> > {
public:
  PYBIND11_TYPE_CASTER(
      katana::CythonReference<T>, _("CythonReference[") + _<T>() + _("]"));

  bool load(handle wrapper, bool) {
    if (pybind11::isinstance(
            wrapper, katana::CythonReference<T>::python_class())) {
      value = katana::CythonReference<T>(wrapper);
      return true;
    }
    return false;
  }

  static handle cast(
      katana::CythonReference<T>& src, return_value_policy, handle) {
    // Must release here to avoid decref'ing the object we are returning making
    // the returned handle invalid.
    return src.wrapper().release();
  }
};
}  // namespace detail
}  // namespace pybind11

namespace katana {

// Cython types which we support.

#define CYTHON_REFERENCE_SUPPORT(type, python_module_name, python_type_name)   \
  template <>                                                                  \
  struct CythonReferenceSupported<type> : std::true_type {                     \
    static pybind11::object python_class() {                                   \
      return pybind11::module::import(python_module_name)                      \
          .attr(python_type_name);                                             \
    }                                                                          \
  }

CYTHON_REFERENCE_SUPPORT(katana::PropertyGraph, "katana.local", "Graph");

#undef CYTHON_REFERENCE_SUPPORT

template <typename T>
pybind11::class_<T>
CythonConstructor(pybind11::class_<T> cls) {
  cls.template def_static(
      "_make_from_address",
      [](uintptr_t addr, pybind11::handle owner [[maybe_unused]]) {
        T* ptr = reinterpret_cast<T*>(addr);
        return ptr;
      },
      pybind11::keep_alive<0, 2>(), pybind11::return_value_policy::reference);
  // keep_alive<0, 2> causes the owner argument to be kept alive as long as the
  // returned object exists. See
  // https://pybind11.readthedocs.io/en/stable/advanced/functions.html#keep-alive
  return cls;
}

}  // namespace katana

#endif
