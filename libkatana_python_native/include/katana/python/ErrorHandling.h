#ifndef KATANA_LIBKATANAPYTHONNATIVE_KATANA_PYTHON_ERRORHANDLING_H_
#define KATANA_LIBKATANAPYTHONNATIVE_KATANA_PYTHON_ERRORHANDLING_H_

#include <katana/Result.h>
#include <pybind11/pybind11.h>

namespace katana::python {
namespace detail {

/// Cast an object to python unless it's already a python object.
template <typename T>
pybind11::object
cast_if_needed(
    T&& v, pybind11::return_value_policy policy, pybind11::handle parent) {
  return pybind11::cast(std::move(v), policy, parent);
}

template <>
inline pybind11::object
cast_if_needed<pybind11::object>(
    pybind11::object&& v, pybind11::return_value_policy, pybind11::handle) {
  return std::move(v);
}

/// Convert @p err to a python exception and raise it.
/// Always throws pybind11::error_already_set.
KATANA_EXPORT pybind11::handle RaiseResultException(
    const katana::ErrorInfo& err);

}  // namespace detail

template <typename T>
T
PythonChecked(const katana::Result<T>& src) {
  if (!src) {
    detail::RaiseResultException(src.error());
  }
  return src.value();
}

template <typename T>
T
PythonChecked(katana::Result<T>&& src) {
  if (!src) {
    detail::RaiseResultException(src.error());
  }
  return std::move(src.value());
}

template <>
inline void
PythonChecked(katana::Result<void>&& src) {
  if (!src) {
    detail::RaiseResultException(src.error());
  }
}

template <typename Cls, typename Return, typename Error, typename... Args>
auto
WithErrorSentinel(Return (Cls::*func)(Args...), Return sentinel, Error error) {
  return [=](Cls& self, Args... args) {
    auto r = self.*func(args...);
    if (r == sentinel) {
      throw error;
    }
    return r;
  };
}

template <typename Cls, typename Return, typename Error, typename... Args>
auto
WithErrorSentinel(
    Return (Cls::*func)(Args...) const, Return sentinel, Error error) {
  return [=](const Cls& self, Args... args) {
    auto r = (self.*func)(args...);
    if (r == sentinel) {
      throw error;
    }
    return r;
  };
}

template <typename Cls, typename Return, typename Error, typename... Args>
auto
WithErrorSentinel(Return (*func)(Cls&, Args...), Return sentinel, Error error) {
  return [=](Cls& self, Args... args) {
    auto r = func(self, args...);
    if (r == sentinel) {
      throw error;
    }
    return r;
  };
}

template <typename Cls, typename Return, typename Error, typename... Args>
auto
WithErrorSentinel(
    Return (*func)(const Cls&, Args...), Return sentinel, Error error) {
  return [=](const Cls& self, Args... args) {
    auto r = func(self, args...);
    if (r == sentinel) {
      throw error;
    }
    return r;
  };
}

}  // namespace katana::python

namespace pybind11 {
namespace detail {

/// Automatic cast from Result<T> to T raising a Python exception if the Result
/// is a failure.
template <typename T>
struct type_caster<katana::Result<T>> {
public:
  PYBIND11_TYPE_CASTER(katana::Result<T>, _<T>());

  bool load(handle, bool) {
    // Conversion always fails since result values cannot originate in Python.
    return false;
  }

  static handle cast(
      katana::Result<T> src, return_value_policy policy, handle parent) {
    if (src) {
      // Must release the object reference (count) to the interpreter when
      // returning. Otherwise, the py::object will decref when it goes out
      // of scope leaving the interpreter with no reference count.
      return katana::python::detail::cast_if_needed(
                 std::move(src.value()), policy, parent)
          .release();
    } else {
      return katana::python::detail::RaiseResultException(src.error());
    }
  }
};

template <>
struct type_caster<katana::Result<void>> {
public:
  PYBIND11_TYPE_CASTER(katana::Result<void>, _<void>());

  bool load(handle, bool) {
    // Conversion always fails since result values cannot originate in Python.
    return false;
  }

  static handle cast(katana::Result<void> src, return_value_policy, handle) {
    if (src) {
      // Must release the object reference (count) to the interpreter when
      // returning. Otherwise, the py::object will decref when it goes out
      // of scope leaving the interpreter with no reference count.
      return pybind11::none().release();
    } else {
      return katana::python::detail::RaiseResultException(src.error());
    }
  }
};

}  // namespace detail
}  // namespace pybind11

#endif
