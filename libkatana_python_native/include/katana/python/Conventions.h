#ifndef KATANA_LIBKATANAPYTHONNATIVE_KATANA_PYTHON_CONVENTIONS_H_
#define KATANA_LIBKATANAPYTHONNATIVE_KATANA_PYTHON_CONVENTIONS_H_

#include <sstream>
#include <type_traits>

#include <fmt/format.h>
#include <pybind11/operators.h>
#include <pybind11/pybind11.h>

#include "katana/CompileTimeIntrospection.h"
#include "katana/OpaqueID.h"

namespace katana {

namespace detail {

// In these classes, the empty implementations are the default cases used when
// the class T does *not* have the method being checked for.

template <typename T>
using has_ostream_insert_t =
    decltype(std::declval<std::ostream>() << std::declval<T>());
template <typename T>
using has_to_string_t = decltype(std::declval<T>().ToString());

/// DefRepr will def `__repr__` based on `ToString` or `<<` if available.
/// `ToString` is preferred.
template <typename ClassT>
void
DefRepr(ClassT& cls) {
  using T = typename ClassT::type;
  if constexpr (is_detected_v<has_to_string_t, T>) {
    cls.def("__repr__", [](T& self) { return self.ToString(); });
  } else if constexpr (is_detected_v<has_ostream_insert_t, T>) {
    cls.def("__repr__", [](const T& self) {
      std::ostringstream out;
      out << self;
      return out.str();
    });
  }
}

template <typename T>
using has_equal_equal_t = decltype(std::declval<T>() == std::declval<T>());
template <typename T>
using has_equals_t = decltype(std::declval<T>().Equals(std::declval<T>()));

template <typename T>
bool
Equals(const T& a, const T& b) {
  if constexpr (is_detected_v<has_equals_t, T>) {
    return a.Equals(b);
  } else if constexpr (is_detected_v<has_equal_equal_t, T>) {
    return a == b;
  } else {
    static_assert(always_false<T>);
  }
}

/// DefEqualsEquals will def ``__eq__`` (Python ``==``) based on Equals() or
/// operator==() if one is available (`Equals` is preferred). Falling back to
/// operator==() is handled by deferring to DefEqualsEquals.
template <typename ClassT>
void
DefEquals(ClassT& cls) {
  using T = typename ClassT::type;
  if constexpr (
      is_detected_v<has_equals_t, T> || is_detected_v<has_equal_equal_t, T>) {
    cls.def("__eq__", [](const T& self, const T& other) {
      return Equals(self, other);
    });
  }
}

template <typename T>
using has_less_than_t = decltype(std::declval<T>() < std::declval<T>());

/// DefComparison will def the python comparison operators based on
/// operator<() if it is available.
template <typename ClassT>
void
DefComparison(ClassT& cls) {
  using T = typename ClassT::type;
  constexpr bool has_equals =
      is_detected_v<has_equals_t, T> || is_detected_v<has_equal_equal_t, T>;
  constexpr bool has_less_than = is_detected_v<has_less_than_t, T>;
  if constexpr (has_equals && has_less_than) {
    cls.def("__lt__", [](const T& a, const T& b) { return a < b; });
    cls.def(
        "__le__", [](const T& a, const T& b) { return a < b || Equals(a, b); });
    cls.def("__gt__", [](const T& a, const T& b) {
      return !(a < b) && !Equals(a, b);
    });
    cls.def("__ge__", [](const T& a, const T& b) { return !(a < b); });
  }
}

template <typename T>
using has_plus_t = decltype(std::declval<T>() + std::declval<T>());
template <typename T>
using has_minus_t = decltype(std::declval<T>() - std::declval<T>());
template <typename T>
using has_unaryminus_t = decltype(-std::declval<T>());

/// DefComparison will def the python comparison operators based on
/// operator<() if it is available.
template <typename ClassT>
void
DefPlusMinus(ClassT& cls) {
  using T = typename ClassT::type;
  if constexpr (is_detected_v<has_plus_t, T>) {
    cls.def(pybind11::self + pybind11::self);
  }
  if constexpr (is_detected_v<has_minus_t, T>) {
    cls.def(pybind11::self - pybind11::self);
  }
  if constexpr (is_detected_v<has_unaryminus_t, T>) {
    cls.def(-pybind11::self);
  }
}

/// DefCopy defs `__copy__` and `copy` based on the copy constructor if it is
/// available.
template <typename ClassT>
void
DefCopy(ClassT& cls) {
  using T = typename ClassT::type;
  if constexpr (std::is_copy_constructible_v<T>) {
    auto copy_operation = [](const T& self) {
      return std::make_unique<T>(self);
    };
    cls.def("__copy__", copy_operation);
    cls.def("copy", copy_operation);
  }
}

template <typename T>
using has_hash_t = decltype(std::hash<T>{}(std::declval<T>()));

/// DefCopy defs `__hash__` if `std::hash` is available.
template <typename ClassT>
void
DefHash(ClassT& cls) {
  using T = typename ClassT::type;
  if constexpr (is_detected_v<has_hash_t, T>) {
    cls.def("__hash__", [](const T& self) { return std::hash<T>{}(self); });
  }
};

template <typename T>
using has_size_t = decltype((size_t)std::declval<T>().size());

template <typename ClassT>
ClassT
DefLen(ClassT& cls) {
  using T = typename ClassT::type;
  if constexpr (is_detected_v<has_size_t, T>) {
    cls.def("__len__", [](T& self) { return self.size(); });
  }
  return cls;
}

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
template <typename ClassT>
ClassT
DefConventions(ClassT cls) {
  detail::DefRepr(cls);
  detail::DefEquals(cls);
  detail::DefComparison(cls);
  detail::DefCopy(cls);
  detail::DefHash(cls);
  return cls;
}

/// Define the __katana_address__ property used by Numba and Cython
/// integrations.
///
/// This is idempotent.
template <typename ClassT>
ClassT
DefKatanaAddress(ClassT& cls) {
  using T = typename ClassT::type;
  if (!hasattr(cls, "__katana_address__")) {
    cls.def_property_readonly(
        "__katana_address__", [](T* self) { return (uintptr_t)self; },
        "Katana Internal. Used for passing objects into numba compiled code.");
  }
  return cls;
}

/// Define an OpaqueID class.
template <typename T>
pybind11::class_<T>
DefOpaqueID(pybind11::module& m, const char* name) {
  pybind11::class_<T> cls(m, name);

  detail::DefEquals(cls);
  detail::DefComparison(cls);
  detail::DefHash(cls);
  detail::DefPlusMinus(cls);

  if constexpr (std::is_base_of_v<
                    katana::OpaqueIDLinear<T, typename T::ValueType>, T>) {
    cls.def(pybind11::self + typename T::DifferenceType());
    cls.def(pybind11::self - typename T::DifferenceType());

    cls.def_property_readonly_static(
        "sentinel", [](pybind11::object) { return T::sentinel(); });
  }

  cls.def(pybind11::init<typename T::ValueType>());
  cls.def_property_readonly("value", [](const T& v) { return v.value(); });

  if constexpr (is_detected_v<detail::has_ostream_insert_t, T>) {
    cls.def("__repr__", [name](const T& self) {
      std::ostringstream out;
      out << name << "(" << self << ")";
      return out.str();
    });
  }

  return cls;
}

/// Define __iter__ on cls using std::begin and std::end to make it an iterable
/// in Python.
template <typename ClassT>
ClassT
DefIterable(ClassT& cls) {
  cls.def("__iter__", [](typename ClassT::type& self) {
    return pybind11::make_iterator(self.begin(), self.end());
  });
  detail::DefLen(cls);
  return cls;
}

template <typename ClassT>
ClassT
DefContainer(ClassT& cls) {
  DefIterable(cls);
  cls.def("__getitem__", [](typename ClassT::type& self, ssize_t i) {
    if (i >= 0) {
      auto iter = self.begin();
      std::advance(iter, i);
      return *iter;
    } else {
      auto iter = self.end();
      std::advance(iter, i);
      return *iter;
    }
  });
  return cls;
}

namespace detail {

/// Return the difference between two elements of the C++ "range" (iterable).
/// This is used to compute the step to allow the Python wrapper to emulate the
/// @e Python @c range type which provides start, stop, and step values.
template <typename T>
auto
GetRangeStep(T& self) -> typename std::remove_reference_t<
    std::remove_cv_t<decltype(*self.begin())>>::difference_type {
  if (std::distance(self.begin(), self.end()) <= 1) {
    return 1;
  }
  auto next = self.begin();
  std::advance(next, 1);
  return *next - *self.begin();
}

}  // namespace detail

template <typename ClassT>
ClassT
DefRange(ClassT& cls) {
  using T = typename ClassT::type;
  static_assert(
      std::is_integral_v<typename std::remove_cv_t<std::remove_reference_t<
          decltype(*std::declval<T>().begin())>>::underlying_type>,
      "Only integral types may be in ranges");
  DefContainer(cls);
  cls.template def_property_readonly(
      "start", [](T& self) { return *self.begin(); });

  cls.template def_property_readonly("step", &detail::GetRangeStep<T>);
  cls.template def_property_readonly(
      "stop", [](T& self) { return *self.end(); });

  cls.template def("__repr__", [cls](T& self) {
    auto step = detail::GetRangeStep(self);
    if (step == 1) {
      return fmt::format(
          FMT_STRING("<{}: {}, {}>"),
          pybind11::cast<std::string>(cls.attr("__name__")), *self.begin(),
          *self.end());
    } else {
      return fmt::format(
          FMT_STRING("<{}: {}, {}, {}>"),
          pybind11::cast<std::string>(cls.attr("__name__")), *self.begin(),
          *self.end(), step);
    }
  });
  return cls;
}

}  // namespace katana

#endif
