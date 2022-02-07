#ifndef KATANA_LIBKATANAPYTHONNATIVE_KATANA_PYTHON_TYPEBANS_H_
#define KATANA_LIBKATANAPYTHONNATIVE_KATANA_PYTHON_TYPEBANS_H_

#include <pybind11/pybind11.h>

namespace katana::python {

// This is always_false, but with a different name to make the error messages
// better.
template <typename... T>
constexpr bool type_is_banned_for_use_with_pybind11 = false;

#define KATANA_PYTHON_BANNED_TYPE_MESSAGE                                      \
  "A type is being passed to into or out of pybind11 that is "                 \
  "not allowed. This is generally used to ban "                                \
  "incorrect smart-pointer types (holders). See the doc comment on "           \
  "katana::python::banned_type_caster."

/// This is used to ban a type from being used in pybind11 wrappers.
/// This should be used to ban incorrect smart-pointer types (holders) from
/// being used. For instance, if PropertyGraph uses shared_ptr as its holder,
/// unique_ptr<PropertyGraph> should be banned to prevent mistakes.
///
/// Usage example:
///
/// @code
/// template <>
///  struct ::pybind11::detail::type_caster<std::unique_ptr<T>> :
///     public katana::python::banned_type_caster<std::unique_ptr<T>> {};
/// ...
///     py::class_<T, std::shared_ptr<T>> cls(m, "T");
/// @endcode
///
/// This bans the usage of std::unique_ptr<T> and sets the holder of T to
/// std::shared_ptr<T>. This avoids many mistakes involving returning pointers
/// using the wrong holder.
///
/// Banning pointers does not work. Only holder class types. This means that
/// accidental duplication of shared_ptr by returning pointers more than once is
/// not possible.
///
/// Banning does not work on the return value of constructor functions. So those
/// py::init functions must match the declared holder to avoid silent failures.
template <typename T>
struct banned_type_caster {
  template <typename U = void>
  bool load(pybind11::handle, bool) {
    static_assert(
        katana::python::type_is_banned_for_use_with_pybind11<T, U>,
        KATANA_PYTHON_BANNED_TYPE_MESSAGE);
    KATANA_LOG_FATAL(KATANA_PYTHON_BANNED_TYPE_MESSAGE);
  }

  template <typename U = void>
  static pybind11::handle cast(
      T, pybind11::return_value_policy, pybind11::handle) {
    static_assert(
        katana::python::type_is_banned_for_use_with_pybind11<T, U>,
        KATANA_PYTHON_BANNED_TYPE_MESSAGE);
    KATANA_LOG_FATAL(KATANA_PYTHON_BANNED_TYPE_MESSAGE);
  }
};

}  // namespace katana::python

#endif
