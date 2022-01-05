#include <katana/Reduction.h>
#include <katana/python/Conventions.h>
#include <katana/python/NumbaSupport.h>
#include <katana/python/PythonModuleInitializers.h>
#include <katana/python/TemplateSupport.h>
#include <pybind11/pybind11.h>

namespace py = pybind11;

namespace {

// The following structures are used to pass reducer *templates* into
// ReducibleFunctor, so it can be used to handle all instantiations of all
// reducers.

// TODO(amp): Use template template parameters once we can rely on having them.
struct ForGAccumulator {
  template <typename T>
  using type = katana::GAccumulator<T>;
};

struct ForReduceMin {
  template <typename T>
  using type = katana::GReduceMin<T>;
};

struct ForReduceMax {
  template <typename T>
  using type = katana::GReduceMax<T>;
};

// The following two structs allow GReduceLogicalOr/And to be passed to
// ReducibleFunctor despite not being templated.
struct ForReduceLogicalOr {
  template <typename T>
  using type = katana::GReduceLogicalOr;
};

struct ForReduceLogicalAnd {
  template <typename T>
  using type = katana::GReduceLogicalAnd;
};

template <typename For>
struct ReducibleFunctor {
  template <typename T>
  py::object instantiate(py::module_& m, const char* name) {
    py::class_<typename For::template type<T>> cls(
        m, name,
        "A reducer object that can be updated with new values and combines the "
        "results efficiently using the appropriate operator.\n"
        "\n"
        "This class can be passed into numba compiled code and it's methods "
        "can be used from there.\n");
    cls.def(py::init<>());
    cls.def(
        py::init([](T v) {
          auto self = std::make_unique<typename For::template type<T>>();
          self->update(v);
          return self;
        }),
        "Create a new instance with an initial value.");
    katana::RegisterNumbaClass(cls);
    katana::DefWithNumba<const T&>(
        cls, "update", &For::template type<T>::update,
        "Update this reducer with ``v`` performing the operation.");
    katana::DefWithNumba<>(
        cls, "reduce", &For::template type<T>::reduce,
        "Get the current value of the reducer. This must only be called from "
        "single threaded code.");
    katana::DefWithNumba<>(
        cls, "get_local", &For::template type<T>::getLocal,
        "Get a sub-result of the reducers operation. This is generally the "
        "reduced value for this thread.");
    katana::DefWithNumba<>(
        cls, "reset", &For::template type<T>::reset,
        "Reset the reducer to its zero. This must only be called from single "
        "threaded code.");
    katana::DefConventions(cls);
    return std::move(cls);
  }
};

}  // namespace

/// Add reduction classes to the module @p m.
void
katana::python::InitReductions(py::module_& m) {
  katana::InstantiateForStandardTypes(
      m, "ReduceSum", ReducibleFunctor<ForGAccumulator>());
  katana::InstantiateForStandardTypes(
      m, "ReduceMax", ReducibleFunctor<ForReduceMax>());
  katana::InstantiateForStandardTypes(
      m, "ReduceMin", ReducibleFunctor<ForReduceMin>());
  // These calls are complex because they would usually be made from the
  // template above. However, it's better than duplicating the implementation of
  // ReducibleFunctor.
  ReducibleFunctor<ForReduceLogicalOr>().instantiate<bool>(m, "ReduceOr");
  ReducibleFunctor<ForReduceLogicalAnd>().instantiate<bool>(m, "ReduceAnd");
}
