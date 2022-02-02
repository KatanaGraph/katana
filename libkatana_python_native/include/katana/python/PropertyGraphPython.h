#ifndef KATANA_LIBKATANAPYTHONNATIVE_KATANA_PYTHON_PROPERTYGRAPHPYTHON_H_
#define KATANA_LIBKATANAPYTHONNATIVE_KATANA_PYTHON_PROPERTYGRAPHPYTHON_H_

#include <arrow/table.h>
#include <pybind11/pybind11.h>

#include "katana/TxnContext.h"

namespace katana::python {

class TxnContextArgumentHandler {
  katana::TxnContext* arg_;
  std::unique_ptr<katana::TxnContext> temp_;

public:
  TxnContextArgumentHandler(katana::TxnContext* arg) {
    if (arg == nullptr) {
      temp_ = std::make_unique<katana::TxnContext>();
      arg_ = temp_.get();
    } else {
      arg_ = arg;
    }
  }

  katana::TxnContext* get() const { return arg_; }

  static constexpr katana::TxnContext* default_value = nullptr;
};

KATANA_EXPORT katana::Result<std::shared_ptr<arrow::Table>>
PythonArgumentsToTable(
    const pybind11::object& table, const pybind11::dict& kwargs);

}  // namespace katana::python

#endif
