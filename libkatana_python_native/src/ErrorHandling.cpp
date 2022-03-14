#include "katana/python/ErrorHandling.h"

namespace py = pybind11;

namespace {

py::object
GetPythonExceptionType(std::error_code code) {
  const auto& katana_category = katana::internal::GetErrorCodeCategory();
  if (code.category() == katana_category) {
    pybind11::module builtins = pybind11::module::import("builtins");
    switch (static_cast<katana::ErrorCode>(code.value())) {
    case katana::ErrorCode::InvalidArgument:
      return builtins.attr("ValueError");
    case katana::ErrorCode::TODO:
    case katana::ErrorCode::NotImplemented:
      return builtins.attr("NotImplementedError");
    case katana::ErrorCode::NotFound:
    case katana::ErrorCode::PropertyNotFound:
      return builtins.attr("LookupError");
    case katana::ErrorCode::TypeError:
      return builtins.attr("TypeError");
    case katana::ErrorCode::AssertionFailed:
      return builtins.attr("AssertionError");
    case katana::ErrorCode::OutOfMemory:
      return builtins.attr("MemoryError");
    case katana::ErrorCode::AlreadyExists:
    case katana::ErrorCode::ArrowError:
    case katana::ErrorCode::JSONParseFailed:
    case katana::ErrorCode::JSONDumpFailed:
    case katana::ErrorCode::HTTPError:
    case katana::ErrorCode::GraphUpdateFailed:
    case katana::ErrorCode::FeatureNotEnabled:
    case katana::ErrorCode::S3Error:
    case katana::ErrorCode::S3ExpiredToken:
    case katana::ErrorCode::AWSWrongRegion:
    case katana::ErrorCode::LocalStorageError:
    case katana::ErrorCode::NoCredentials:
    case katana::ErrorCode::AzureError:
    case katana::ErrorCode::BadVersion:
    case katana::ErrorCode::MpiError:
    case katana::ErrorCode::GSError:
      break;
    }
  }
  auto katana_module = pybind11::module::import("katana");
  return katana_module.attr(code.category().name());
}

}  // namespace

pybind11::handle
katana::python::detail::RaiseResultException(const katana::ErrorInfo& err) {
  py::gil_scoped_acquire with_gil;
  std::ostringstream ss;
  err.Write(ss);
  auto code = err.error_code();
  pybind11::object error_type;
  try {
    error_type = GetPythonExceptionType(code);
    PyErr_SetString(error_type.ptr(), ss.str().c_str());
  } catch (pybind11::error_already_set& eas) {
    ss << " (error code category is " << code.category().name()
       << " which does not have a custom exception class)";
    error_type =
        pybind11::reinterpret_borrow<pybind11::object>(PyExc_RuntimeError);
    raise_from(eas, error_type.ptr(), ss.str().c_str());
  }
  throw pybind11::error_already_set();
}
