#ifndef KATANA_PYTHON_KATANA_LOCALNATIVE_CONVENTIONS_H_
#define KATANA_PYTHON_KATANA_LOCALNATIVE_CONVENTIONS_H_

#include <type_traits>

#include <pybind11/pybind11.h>

namespace katana {

namespace detail {

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

}  // namespace detail

template <typename T>
pybind11::class_<T>
DefConventions(pybind11::class_<T>&& cls) {
  detail::DefRepr<T>{}(cls);
  detail::DefEquals<T>{}(cls);
  return std::move(cls);
}

}  // namespace katana

#endif
