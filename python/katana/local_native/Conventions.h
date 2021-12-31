#ifndef KATANA_PYTHON_KATANA_LOCALNATIVE_CONVENTIONS_H_
#define KATANA_PYTHON_KATANA_LOCALNATIVE_CONVENTIONS_H_

#include <type_traits>

#include <pybind11/pybind11.h>

namespace katana {

namespace detail {

/// DefRepr will def `__repr__` based on `ToString` if it is available.
template <typename T, typename Enable = void>
struct DefRepr {
  void operator()(pybind11::class_<T>& cls [[maybe_unused]]) {}
};

template <typename T>
struct DefRepr<T, std::void_t<decltype(std::declval<T>().ToString())>> {
  void operator()(pybind11::class_<T>& cls) {
    cls.def("__repr__", &T::ToString);
  }
};

/// DefEqualsEquals will def `__eq__` based on `operator==` if it is available.
template <typename T, typename Enable = void>
struct DefEqualsEquals {
  void operator()(pybind11::class_<T>& cls [[maybe_unused]]) {}
};

template <typename T>
struct DefEqualsEquals<
    T, std::void_t<decltype(std::declval<T>() == std::declval<T>())>> {
  void operator()(pybind11::class_<T>& cls) {
    cls.def("__eq__", &T::operator==);
  }
};

/// DefEqualsEquals will def `__eq__` based on `Equals` or `operator==` if one
/// is available (`Equals` is preferred).
template <typename T, typename Enable = void>
struct DefEquals {
  void operator()(pybind11::class_<T>& cls) {
    // If Equals doesn't exist then check for ==.
    DefEqualsEquals<T>{}(cls);
  }
};

template <typename T>
struct DefEquals<
    T, std::void_t<decltype(std::declval<T>().Equals(std::declval<T>()))>> {
  void operator()(pybind11::class_<T>& cls) { cls.def("__eq__", &T::Equals); }
};

/// DefCopy defs `__copy__` and `copy` based on the copy constructor if it is
/// available.
template <typename T, typename Enable = void>
struct DefCopy {
  void operator()(pybind11::class_<T>& cls [[maybe_unused]]) {}
};

template <typename T>
struct DefCopy<T, std::void_t<decltype(T((const T&)std::declval<T>()))>> {
  void operator()(pybind11::class_<T>& cls) {
    cls.def(
        "__copy__", [](const T& self) { return std::make_unique<T>(self); });
    cls.def("copy", [](const T& self) { return std::make_unique<T>(self); });
  }
};

}  // namespace detail

/// Define Python members of cls based on which methods the C++ type T provides.
///
/// This will attempt to define:
///
/// @li __repr__
/// @li __eq__
/// @li __copy__
///
/// \tparam T The C++ type wrapped by @p cls. Usually inferred.
/// \param cls The pybind11 class object.
/// \return @p cls to allow chaining.
template <typename T>
pybind11::class_<T>
DefConventions(pybind11::class_<T>& cls) {
  detail::DefRepr<T>{}(cls);
  detail::DefEquals<T>{}(cls);
  detail::DefCopy<T>{}(cls);
  return cls;
}

}  // namespace katana

#endif
