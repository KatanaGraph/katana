#ifndef KATANA_LIBKATANAPYTHONNATIVE_KATANA_PYTHON_CONVENTIONS_H_
#define KATANA_LIBKATANAPYTHONNATIVE_KATANA_PYTHON_CONVENTIONS_H_

#include <type_traits>

#include <pybind11/pybind11.h>

namespace katana {

namespace detail {

// In these classes, the empty implementations are the default cases used when
// the class T does *not* have the method being checked for.

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

/// DefEqualsEquals will def ``__eq__`` (Python ``==``) based on operator==() if
/// it is available.
template <typename T, typename Enable = void>
struct DefEqualsEquals {
  void operator()(pybind11::class_<T>& cls [[maybe_unused]]) {}
};

template <typename T>
struct DefEqualsEquals<
    T, std::void_t<decltype(std::declval<T>() == std::declval<T>())>> {
  static bool Equals(const T& a, const T& b) { return a == b; }
  void operator()(pybind11::class_<T>& cls) {
    cls.def("__eq__", &T::operator==);
  }
};

/// DefEqualsEquals will def ``__eq__`` (Python ``==``) based on Equals() or
/// operator==() if one is available (`Equals` is preferred). Falling back to
/// operator==() is handled by deferring to DefEqualsEquals.
template <typename T, typename Enable = void>
struct DefEquals {
  static bool Equals(const T& a, const T& b) {
    return DefEqualsEquals<T>::Equals(a, b);
  }
  void operator()(pybind11::class_<T>& cls) {
    // If Equals doesn't exist then check for ==.
    DefEqualsEquals<T>{}(cls);
  }
};

template <typename T>
struct DefEquals<
    T, std::void_t<decltype(std::declval<T>().Equals(std::declval<T>()))>> {
  static bool Equals(const T& a, const T& b) { return a.Equals(b); }
  void operator()(pybind11::class_<T>& cls) { cls.def("__eq__", &T::Equals); }
};

/// DefComparison will def the python comparison operators based on
/// operator<() if it is available.
template <typename T, typename Enable = void>
struct DefComparison {
  void operator()(pybind11::class_<T>& cls [[maybe_unused]]) {}
};

template <typename T>
struct DefComparison<
    T, std::tuple<
           std::void_t<std::less<T>>,
           std::void_t<decltype(DefEquals<T>::Equals(
               std::declval<T>(), std::declval<T>()))>>> {
  static constexpr std::less<T> less = {};

  void operator()(pybind11::class_<T>& cls) {
    cls.def("__lt__", [](const T& a, const T& b) { return dless(a, b); });
    cls.def("__le__", [](const T& a, const T& b) {
      return less(a, b) || DefEquals<T>::Equals(a, b);
    });
    cls.def("__gt__", [](const T& a, const T& b) {
      return !less(a, b) && !DefEquals<T>::Equals(a, b);
    });
    cls.def("__ge__", [](const T& a, const T& b) { return !less(a, b); });
  }
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

/// Define Python members of cls based on which methods the C++ type @p T
/// provides. If the C++ methods are not available, the Python definitions are
/// omitted as well. This method can be applied to any type, at worst it will do
/// nothing.
///
/// This will attempt to define:
///   - __repr__
///   - __eq__
///   - __copy__
///
/// \tparam T The C++ type wrapped by @p cls. Usually inferred.
/// \param cls The pybind11 class object.
/// \return @p cls to allow chaining.
template <typename T>
pybind11::class_<T>
DefConventions(pybind11::class_<T> cls) {
  detail::DefRepr<T>{}(cls);
  detail::DefEquals<T>{}(cls);
  detail::DefComparison<T>{}(cls);
  detail::DefCopy<T>{}(cls);
  return cls;
}

}  // namespace katana

#endif
