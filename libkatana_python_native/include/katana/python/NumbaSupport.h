#ifndef KATANA_LIBKATANAPYTHONNATIVE_KATANA_PYTHON_NUMBASUPPORT_H_
#define KATANA_LIBKATANAPYTHONNATIVE_KATANA_PYTHON_NUMBASUPPORT_H_

#include <type_traits>

#include <katana/python/TypeTraits.h>
#include <pybind11/pybind11.h>

namespace katana {

namespace detail {

// TODO(amp): Use std::remove_cvref_t when/if we are only supporting C++20.
template <typename T>
using remove_cvref_t = std::remove_cv_t<std::remove_reference_t<T>>;

/// A utility class for instantiating static function wrappers and binding
/// them into Python. It encapsulates much of the type trickery.
template <typename _Return, typename... _Args>
struct StaticFunctionBinderImpl {
  using Return = _Return;
  using StaticFunctionPointer = Return (*)(_Args...);

  /// A wrapper function that makes sure any argument conversions required by
  /// the use of remove_cvref are performed on each call.
  template <StaticFunctionPointer func>
  static remove_cvref_t<Return> call(remove_cvref_t<_Args>... args) {
    return func(args...);
  }

  /// Define an actual python method and numba wrapper using @p func.
  template <StaticFunctionPointer func, typename... Extra>
  static void def_method(
      pybind11::module& m, const char* name, const Extra&... extra) {
    m.def(name, func, extra...);
    auto numba_support =
        pybind11::module::import("katana.native_interfacing.numba_support");
    numba_support.attr("register_function")(
        m.attr(name), (uintptr_t)&call<func>,
        PythonTypeTraits<remove_cvref_t<Return>>::ctypes_type(),
        PythonTypeTraits<remove_cvref_t<_Args>>::ctypes_type()...);
  }
};

/// A function used to pattern match on a type to extract the return
/// and argument types as a binder object.
template <typename Return, typename... Args>
constexpr auto
StaticFunctionBinderInferer(Return (*)(Args...)) {
  return StaticFunctionBinderImpl<Return, Args...>{};
}

/// A template to get the type of the required binder using a function pointer
/// passed as a template argument.
template <auto func>
using StaticFunctionBinder = decltype(StaticFunctionBinderInferer(func));

/// A utility class encapsulating the type manipulation required for binding
/// non-const member functions.
template <typename _Cls, typename _Return, typename... _Args>
struct MemberFunctionBinderImpl {
  using Cls = _Cls;
  using Return = _Return;
  template <typename Impl>
  using BaseMemberFunctionPointer = Return (Impl::*)(_Args...);
  template <typename Impl>
  using BaseStaticFunctionPointer = Return (*)(Impl*, remove_cvref_t<_Args>...);
  using MemberFunctionPointer = BaseMemberFunctionPointer<Cls>;
  using StaticFunctionPointer = BaseStaticFunctionPointer<Cls>;

  /// A static wrapper function which forwards calls to the statically provided
  /// member function pointer.
  template <MemberFunctionPointer member_func>
  static remove_cvref_t<Return> call(Cls* self, remove_cvref_t<_Args>... args) {
    return (self->*(member_func))(args...);
  }

  /// Define an actual python method and numba wrapper using @p member_func.
  template <
      MemberFunctionPointer member_func, typename SelfCls, typename... Extra>
  static void def_class_method(
      pybind11::class_<SelfCls>& cls, const char* name, const Extra&... extra) {
    cls.def(name, member_func, extra...);
    auto numba_support =
        pybind11::module::import("katana.native_interfacing.numba_support");
    numba_support.attr("register_method")(
        cls, cls.attr(name), (uintptr_t)&call<member_func>,
        PythonTypeTraits<remove_cvref_t<Return>>::ctypes_type(),
        PythonTypeTraits<remove_cvref_t<_Args>>::ctypes_type()...);
  }

  constexpr static bool is_const = false;
};

/// A utility class encapsulating the type manipulation required for binding
/// const member functions.
template <typename _Cls, typename _Return, typename... _Args>
struct ConstMemberFunctionBinderImpl {
  using Cls = _Cls;
  using Return = _Return;
  template <typename Impl>
  using BaseMemberFunctionPointer =
      Return (Impl::*)(remove_cvref_t<_Args>...) const;
  template <typename Impl>
  using BaseStaticFunctionPointer =
      Return (*)(const Impl*, remove_cvref_t<_Args>...);
  using MemberFunctionPointer = BaseMemberFunctionPointer<Cls>;
  using StaticFunctionPointer = BaseStaticFunctionPointer<Cls>;

  template <MemberFunctionPointer member_func>
  static remove_cvref_t<Return> call(
      const Cls* self, remove_cvref_t<_Args>... args) {
    return (self->*(member_func))(args...);
  }

  /// Define an actual python method and numba wrapper using @p member_func.
  template <
      MemberFunctionPointer member_func, typename SelfCls, typename... Extra>
  static void def_class_method(
      pybind11::class_<SelfCls>& cls, const char* name, const Extra&... extra) {
    cls.def(name, member_func, extra...);
    auto numba_support =
        pybind11::module::import("katana.native_interfacing.numba_support");
    numba_support.attr("register_method")(
        cls, cls.attr(name), (uintptr_t)&call<member_func>,
        PythonTypeTraits<remove_cvref_t<Return>>::ctypes_type(),
        PythonTypeTraits<remove_cvref_t<_Args>>::ctypes_type()...);
  }

  constexpr static bool is_const = true;
};

template <typename Cls, typename Return, typename... Args>
constexpr auto
MemberFunctionBinderInferer(Return (Cls::*)(Args...)) {
  return MemberFunctionBinderImpl<Cls, Return, Args...>{};
}

template <typename Cls, typename Return, typename... Args>
constexpr auto
MemberFunctionBinderInferer(Return (Cls::*)(Args...) const) {
  return ConstMemberFunctionBinderImpl<Cls, Return, Args...>{};
}

template <auto member_func>
using MemberFunctionBinder = decltype(MemberFunctionBinderInferer(member_func));

}  // namespace detail

/// Define a Python and numba method.
/// This is used as follows:
/// @code
/// DefWithNumba&lt;&func>(cls, "func");
/// @endcode
/// If the method is overloaded, you must select a specific overload using
/// pybind11::overload_cast().
/// @code
/// DefWithNumba&lt;pybind11:overload_cast&lt;const std::string&>(&func)>(cls, "func");
/// @endcode
///
/// Numba interfacing does not (yet) support exposing overloading itself to
/// Numba code.
///
/// \tparam func The function pointer @b value (not type).
/// \param cls The python class object.
/// \param name The name of the method in python.
/// \param extra Extra arguments as you would pass to pybind11::class_::def().
template <auto func, typename... Extra>
constexpr void
DefWithNumba(pybind11::module& m, const char* name, const Extra&... extra) {
  static_assert(
      std::is_same_v<
          typename detail::StaticFunctionBinder<func>::StaticFunctionPointer,
          decltype(func)>);
  detail::StaticFunctionBinder<func>::template def_method<func>(
      m, name, extra...);
}

/// Define a Python and numba method.
/// This is used as follows:
/// @code
/// DefWithNumba&lt;&Cls::method>(cls, "method");
/// @endcode
/// If the method is overloaded, you must select a specific overload using
/// pybind11::overload_cast().
/// @code
/// DefWithNumba&lt;pybind11:overload_cast&lt;const std::string&>(&Cls::method)>(cls, "method");
/// @endcode
///
/// Numba interfacing does not (yet) support exposing overloading itself to
/// Numba code.
///
/// \tparam member_func The member function pointer @b value (not type).
/// \param cls The Python class object.
/// \param name The name of the method in python.
/// \param extra Extra arguments as you would pass to pybind11::class_::def().
template <auto member_func, typename Cls, typename... Extra>
constexpr void
DefWithNumba(
    pybind11::class_<Cls>& cls, const char* name, const Extra&... extra) {
  static_assert(std::is_same_v<
                typename detail::MemberFunctionBinder<
                    member_func>::MemberFunctionPointer,
                decltype(member_func)>);
  detail::MemberFunctionBinder<member_func>::template def_class_method<
      member_func>(cls, name, extra...);
}

// TODO(amp): Implement missing overloads/variants for: lambdas (as methods and
//  functions), static methods.

/// Register a Python class for use from Numba compiled code.
/// This enables DefWithNumba() to be used on methods of this class.
///
/// This calls `katana.native_interfacing.numba_support.register_class`
///
/// \param cls The Python class object.
template <typename T>
void
RegisterNumbaClass(pybind11::class_<T>& cls) {
  cls.def_property_readonly(
      "__katana_address__", [](T* self) { return (uintptr_t)self; });
  auto numba_support =
      pybind11::module::import("katana.native_interfacing.numba_support");
  numba_support.attr("register_class")(cls);
}

}  // namespace katana

#endif
