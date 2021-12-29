#include <katana/Reduction.h>
#include <katana/SharedMemSys.h>
#include <pybind11/pybind11.h>

#include "NumbaSupport.h"
#include "TemplateSupport.h"

namespace py = pybind11;

namespace {

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
    katana::RegisterNumbaClass(cls);
    katana::DefWithNumba<const T&>(
        cls, "update", &For::template type<T>::update);
    katana::DefWithNumba<>(cls, "reduce", &For::template type<T>::reduce);
    katana::DefWithNumba<>(cls, "reset", &For::template type<T>::reset);
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
