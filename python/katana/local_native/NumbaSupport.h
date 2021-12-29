#ifndef KATANA_PYTHON_KATANA_LOCALNATIVE_NUMBASUPPORT_H_
#define KATANA_PYTHON_KATANA_LOCALNATIVE_NUMBASUPPORT_H_

#include <type_traits>

#include <pybind11/pybind11.h>

#include "TypeTraits.h"

namespace katana {

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

  static Return call(
      MemberFunction<Cls, Return, Args...>* func, Cls* self, Args... args) {
    return (self->*(func->member_func))(args...);
  }
};

template <typename Cls, typename Return, typename... Args>
struct ConstMemberFunction {
  Return (Cls::*member_func)(Args...) const;

  static Return call(
      ConstMemberFunction<Cls, Return, Args...>* func, const Cls* self,
      Args... args) {
    return (self->*(func->member_func))(args...);
  }
};

// TODO(amp): Use std::remove_cvref_t when/if we are only supporting C++20.
template <typename T>
using remove_cvref_t = std::remove_cv_t<std::remove_reference_t<T>>;

template <typename... Args>
class DefWithNumbaImpl {
  template <typename Cls, typename Return, typename Func, typename Caller>
  void def_class_method(
      pybind11::class_<Cls>& cls, const char* name, Func f,
      Caller* caller) const {
    cls.def(name, f);
    auto numba =
        pybind11::module_::import("katana.native_interfacing.numba_support");
    numba.attr("register_method")(
        cls, cls.attr(name), (uintptr_t)caller, (uintptr_t)&Caller::call,
        PythonTypeTraits<remove_cvref_t<Return>>::representation(),
        PythonTypeTraits<remove_cvref_t<Args>>::representation()...);
  }

  template <typename Return>
  void def_func(
      pybind11::module_& m, const char* name, Return (*f)(Args...)) const {
    m.def(name, f);
    auto func = m.attr(name);
    auto numba =
        pybind11::module_::import("katana.native_interfacing.numba_support");
    numba.attr("register_function")(
        func, (uintptr_t)f,
        PythonTypeTraits<remove_cvref_t<Return>>::representation(),
        PythonTypeTraits<remove_cvref_t<Args>>::representation()...);
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

  template <typename Cls, typename Func>
  void operator()(
      pybind11::class_<Cls>& cls, const char* name, Func&& f) const {
    cls.def(name, f);
    auto ff = [](Func* f_ptr, Cls* self, Args... args) {
      return (*f_ptr)(self, args...);
    };
    auto numba =
        pybind11::module_::import("katana.native_interfacing.numba_support");
    numba.attr("register_method")(
        cls, cls.attr(name), (uintptr_t)&f, (uintptr_t)&ff,
        PythonTypeTraits<remove_cvref_t<pybind11::detail::remove_class<
            decltype(Func::operator())>>>::representation(),
        PythonTypeTraits<remove_cvref_t<Args>>::representation()...);
  }
};

template <typename... Args>
constexpr DefWithNumbaImpl<Args...> DefWithNumba{};

template <typename T>
void
RegisterNumbaClass(pybind11::class_<T>& cls) {
  cls.def_property_readonly(
      "__katana_address__", [](T* self) { return (uintptr_t)self; });
}

}  // namespace katana

#endif
