#include <katana/Reduction.h>
#include <katana/python/Conventions.h>
#include <katana/python/NumbaSupport.h>
#include <katana/python/PythonModuleInitializers.h>
#include <katana/python/TemplateSupport.h>
#include <pybind11/pybind11.h>

namespace py = pybind11;

namespace {

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
  py::object operator()(py::module_& m, const char* name) {
    py::class_<typename For::template type<T>> cls(m, name);
    cls.def(py::init<>());
    cls.def(py::init([](T v) {
      auto self = std::make_unique<typename For::template type<T>>();
      self->update(v);
      return self;
    }));
    katana::RegisterNumbaClass(cls);
    katana::DefWithNumba<const T&>(
        cls, "update", &For::template type<T>::update);
    katana::DefWithNumba<>(cls, "reduce", &For::template type<T>::reduce);
    katana::DefWithNumba<>(cls, "get_local", &For::template type<T>::getLocal);
    katana::DefWithNumba<>(cls, "reset", &For::template type<T>::reset);
    katana::DefConventions(cls);
    return std::move(cls);
  }
};

}  // namespace

void
InitReductions(py::module_& m) {
  katana::InstantiateForStandardTypes(
      m, "ReduceSum", ReducibleFunctor<ForGAccumulator>());
  katana::InstantiateForStandardTypes(
      m, "ReduceMax", ReducibleFunctor<ForReduceMax>());
  katana::InstantiateForStandardTypes(
      m, "ReduceMin", ReducibleFunctor<ForReduceMin>());
  ReducibleFunctor<ForReduceLogicalOr>().operator()<bool>(m, "ReduceOr");
  ReducibleFunctor<ForReduceLogicalAnd>().operator()<bool>(m, "ReduceAnd");
}
