/// @warning This code is temporary and will be removed once all Cython wrappers
///          are removed.
///
/// The utilities in this module allow pybind11 and Cython to access and
/// construct each others objects. The implementation has some performance
/// issues (repeated python function calls), and it cannot be used within numba
/// compiled code.
///
/// The interface provided by both pybind11 and Cython objects entirely call
/// via Python to avoid the need to have an ABI between pybind11 and Cython
/// code. The interface is:
///
/// * an instance property __katana_address__ which returns the address of the
///   underlying C++ object as an int.
/// * a static method _make_from_address which takes the C++ object pointer as
///   an int, and returns a new Python object wrapping it.
///
/// Any other interaction between pybind11 and Cython must happen via the
/// existing C++ or Python interfaces. Either can of course call methods in
/// either langauge.
#ifndef KATANA_LIBKATANAPYTHONNATIVE_KATANA_PYTHON_CYTHONINTEGRATION_H_
#define KATANA_LIBKATANAPYTHONNATIVE_KATANA_PYTHON_CYTHONINTEGRATION_H_

#include <katana/Bag.h>
#include <katana/PropertyGraph.h>
#include <pybind11/pybind11.h>

namespace katana {

/// A trait class used to mark classes @p $ that can be translated from Cython.
/// Specializations should derive from std::true_type and provide a static
/// method pybind11::object python_class() which returns the Python class object
/// of the Cython class associated with @p T.
template <typename T>
struct CythonReferenceSupported : std::false_type {};

/// A reference to a Cython class instance wrapping an instance of @p T.
/// This class should be used like a smart-pointer. It always owns the
/// underlying instance, so the raw pointer should not be allowed to outlive
/// this.
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

/// CYTHON_REFERENCE_SUPPORT(C++ type,
///      the name of the module holding the Cython class object as a string,
///      the name of the Cython class as a string);
#define CYTHON_REFERENCE_SUPPORT(type, python_module_name, python_type_name)   \
  template <>                                                                  \
  struct CythonReferenceSupported<type> : std::true_type {                     \
    static pybind11::object python_class() {                                   \
      return pybind11::module::import(python_module_name)                      \
          .attr(python_type_name);                                             \
    }                                                                          \
  }

// Add Cython classes here as needed. Remove them when they are moved to
// pybind11.
CYTHON_REFERENCE_SUPPORT(katana::PropertyGraph, "katana.local", "Graph");

#undef CYTHON_REFERENCE_SUPPORT

/// Define utilities on @p cls which allow Cython to access and construct
/// instances of the pybind11 wrapper of @p T.
template <typename T>
pybind11::class_<T>
DefCythonSupport(pybind11::class_<T> cls) {
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
  DefKatanaAddress(cls);
  return cls;
}

}  // namespace katana

#endif
