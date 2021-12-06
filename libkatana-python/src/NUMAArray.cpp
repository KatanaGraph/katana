#include "katana/NUMAArray.h"

#include <pybind11/pybind11.h>

#include "AllModules.h"
#include "ErrorHandling.h"
//#include "NumbaSupport.h"
#include "Numpy.h"
#include "TemplateSupport.h"

namespace py = pybind11;

// TODO(amp): Implement a NUMA buffer object which provides the python buffer
//  protocol and can easily be used as a safe allocator for numpy arrays.

struct DeclareNUMAArray_t {
  template <typename T>
  py::object operator()(py::module_& m, const char* name) {
    pybind11::class_<katana::NUMAArray<T>> numa_array_cls(m, name);

    pybind11::enum_<typename katana::NUMAArray<T>::AllocType>(
        numa_array_cls, "AllocType")
        .value("Blocked", katana::NUMAArray<T>::AllocType::Blocked)
        .value("Local", katana::NUMAArray<T>::AllocType::Local)
        .value("Interleaved", katana::NUMAArray<T>::AllocType::Interleaved)
        .value("Floating", katana::NUMAArray<T>::AllocType::Floating)
        .export_values();

    numa_array_cls
        .def(
            py::init([](typename katana::NUMAArray<T>::size_type size,
                        typename katana::NUMAArray<T>::AllocType alloc_type) {
              auto ret = new katana::NUMAArray<T>();
              ret->Allocate(size, alloc_type);
              return ret;
            }),
            py::arg("size"), py::arg("alloc_type"))
        .def(
            "as_numpy",
            [](katana::NUMAArray<T>& self) {
              return katana::AsNumpy(self.data(), self.size());
            },
            py::return_value_policy::reference_internal);

    //    katana::RegisterNumbaClass(numa_array_cls);
    //
    //    katana::DefWithNumba<typename katana::NUMAArray<T>::difference_type>(
    //        numa_array_cls, "__getitem__", &katana::NUMAArray<T>::at, py::const_);
    //    katana::DefWithNumba<size_t, T>(
    //        numa_array_cls, "__setitem__",
    //        [](katana::NUMAArray<T>* self, size_t i, T v) { (*self)[i] = v; });

    return std::move(numa_array_cls);
  }
};

void
InitNUMAArray(py::module_& m) {
  katana::InstantiateForStandardTypes(m, "NUMAArray", DeclareNUMAArray_t());
}
