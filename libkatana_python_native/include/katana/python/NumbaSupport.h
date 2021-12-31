#ifndef KATANA_LIBKATANAPYTHONNATIVE_KATANA_PYTHON_NUMBASUPPORT_H_
#define KATANA_LIBKATANAPYTHONNATIVE_KATANA_PYTHON_NUMBASUPPORT_H_

#include <type_traits>

#include <katana/python/TypeTraits.h>
#include <pybind11/pybind11.h>

namespace katana {

// TODO(amp): Use std::remove_cvref_t when/if we are only supporting C++20.
template <typename T>
using remove_cvref_t = std::remove_cv_t<std::remove_reference_t<T>>;

/// A wrapper around a member function pointer which (unlike std::mem_fn) allows
/// calls to the function via a static function pointer allowing it to be
/// called from Numba generated code.
///
/// @tparam Cls The class containing the method.
/// @tparam Return The return value of the method.
/// @tparam Args The argument types to the method.
template <typename Cls, typename Return, typename... Args>
struct MemberFunction {
  Return (Cls::*member_func)(Args...);

  static remove_cvref_t<Return> call(
      MemberFunction<Cls, Return, Args...>* func, Cls* self,
      remove_cvref_t<Args>... args) {
    return (self->*(func->member_func))(args...);
  }
};

template <typename Cls, typename Return, typename... Args>
struct ConstMemberFunction {
  Return (Cls::*member_func)(Args...) const;

  static remove_cvref_t<Return> call(
      ConstMemberFunction<Cls, Return, Args...>* func, const Cls* self,
      remove_cvref_t<Args>... args) {
    return (self->*(func->member_func))(args...);
  }
};

template <typename... Args>
class DefWithNumbaImpl {
  template <typename Cls, typename Return, typename Func, typename Caller>
  void def_class_method(
      pybind11::class_<Cls>& cls, const char* name, Func f,
      Caller* caller) const {
    cls.def(name, f);
    auto numba_support =
        pybind11::module_::import("katana.native_interfacing.numba_support");
    numba_support.attr("register_method")(
        cls, cls.attr(name), (uintptr_t)&Caller::call, (uintptr_t)caller,
        PythonTypeTraits<remove_cvref_t<Return>>::ctypes_type(),
        PythonTypeTraits<remove_cvref_t<Args>>::ctypes_type()...);
  }

  template <typename Return>
  void def_func(
      pybind11::module_& m, const char* name, Return (*f)(Args...)) const {
    m.def(name, f);
    auto func = m.attr(name);
    auto numba_support =
        pybind11::module_::import("katana.native_interfacing.numba_support");
    numba_support.attr("register_function")(
        func, (uintptr_t)f, 0,
        PythonTypeTraits<remove_cvref_t<Return>>::ctypes_type(),
        PythonTypeTraits<remove_cvref_t<Args>>::ctypes_type()...);
  }

public:
  // TODO(amp): The generated wrappers used from numba code are created per
  //  *signature*, not per function. So two functions will the same overall
  //  signature (including containing class) will use the same wrapper. This
  //  could possibly cause a megamorphic call site that defeats the CPU branch
  //  predictor. The fix for this is to somehow distinguish different
  //  functions/methods at the template level. This could be done with a counter
  //  or a name argument of some kind.

  template <typename Return>
  void operator()(
      pybind11::module_& m, const char* name, Return (*pf)(Args...)) const {
    def_func(m, name, pf);
  }

  template <typename Func>
  void operator()(pybind11::module_& m, const char* name, Func&& f) const {
    this->template operator()(m, name, &f);
  }

  template <typename Cls, typename Return, typename ImplCls>
  void operator()(
      pybind11::class_<Cls>& cls, const char* name,
      Return (ImplCls::*pmf)(Args...), std::false_type = {}) const {
    // This leaks a single pointer sized struct for each defined numba function.
    // Repeated import could theoretically cause this to matter, but it's very
    // unlikely.
    def_class_method<Cls, Return>(
        cls, name, pmf, new MemberFunction<ImplCls, Return, Args...>{pmf});
  }

  template <typename Cls, typename Return, typename ImplCls>
  void operator()(
      pybind11::class_<Cls>& cls, const char* name,
      Return (ImplCls::*pmf)(Args...) const, std::true_type) const {
    // As above.
    def_class_method<Cls, Return>(
        cls, name, pmf, new ConstMemberFunction<ImplCls, Return, Args...>{pmf});
  }

  //  template <typename Cls, typename Func>
  //  void operator()(
  //      pybind11::class_<Cls>& cls, const char* name, Func&& f) const {
  //    cls.def(name, f);
  //    auto ff = [](Func* f_ptr, Cls* self, Args... args) {
  //      return (*f_ptr)(self, args...);
  //    };
  //    auto numba =
  //        pybind11::module_::import("katana.native_interfacing.numba_support");
  //    numba.attr("register_method")(
  //        cls, cls.attr(name), (uintptr_t)&f, (uintptr_t)&ff,
  //        PythonTypeTraits<remove_cvref_t<pybind11::detail::remove_class<
  //            decltype(Func::operator())>>>::representation(),
  //        PythonTypeTraits<remove_cvref_t<Args>>::representation()...);
  //  }
};

/// Declare a method or function to be called from Numba and Python.
///
/// This should be called the same way `pybind11`'s `def` function is called.
///
/// \tparam Args The argument types of the function.
template <typename... Args>
constexpr DefWithNumbaImpl<Args...> DefWithNumba{};

/// Register a Python class for use from Numba compiled code.
///
/// This calls `katana.native_interfacing.numba_support.register_class`
template <typename T>
void
RegisterNumbaClass(pybind11::class_<T>& cls) {
  cls.def_property_readonly(
      "__katana_address__", [](T* self) { return (uintptr_t)self; });
  auto numba_support =
      pybind11::module_::import("katana.native_interfacing.numba_support");
  numba_support.attr("register_class")(cls);
}

}  // namespace katana

#endif
