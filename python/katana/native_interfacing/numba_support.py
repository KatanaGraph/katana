"""A set of functions for registering classes, methods, and functions from Numba compiled code.

The primary user of this module is the `pybind11` code in `NumbaSupport.h`.
"""

import ctypes

from katana.native_interfacing import exec_in_file
from katana.native_interfacing.wrappers import SimpleNumbaPointerWrapper


def register_class(cls: type) -> None:
    """Configure ``cls`` to support Numba methods.

    :param cls: The class to configure. It must have a ``__katana_address__`` property which returns the pointer to the
        native object as an `int`.
    """
    if not hasattr(cls, "__katana_address__"):
        raise ValueError("Numba supporting classes must have a __katana_address__ property.")
    cls._numba_type_wrapper = SimpleNumbaPointerWrapper(cls)


def register_method(cls: type, method, invoker_ptr: int, ret_type, *arg_types) -> None:
    """Register a function pointer to implement ``method`` when called from Numba compiled code.

    ``cls`` must have been registered with `register_class`.

    :param cls: The class containing the method.
    :param method: The method to register.
    :type method: unbound method.
    :param invoker_ptr: The pointer to the invoker function.
    :param ret_type: The type of the method return value.
    :type ret_type: `ctype` type object or None.
    :param arg_types: The types of each argument value.
    :type arg_types: `ctype` type object.
    """
    if not hasattr(cls, "_numba_type_wrapper"):
        raise ValueError(
            "Classes with numba methods must be registered as numba classes with "
            "katana.native_interfacing.numba_support.register_class (in C++, katana::RegisterNumbaClass)."
        )
    numba_wrapper: SimpleNumbaPointerWrapper = cls._numba_type_wrapper
    ctypes_func_type = ctypes.CFUNCTYPE(ret_type, ctypes.c_void_p, *arg_types)
    if hasattr(method, "__name__"):
        name = method.__name__
    else:
        name = method
    numba_wrapper.register_method(name, ctypes_func_type, addr=invoker_ptr)


def register_function(func: callable, invoker_ptr: int, ret_type, *arg_types) -> None:
    """Register a function pointer to implement ``func`` when called from Numba compiled code.

    :param func: The Python function to register.
    :param invoker_ptr: The pointer to the invoker function.
    :param ret_type: The type of the method return value.
    :type ret_type: `ctype` type object or None.
    :param arg_types: The types of each argument value.
    :type arg_types: `ctype` type object.
    """
    raise NotImplementedError("Top level functions are not yet supported by the numba wrapper framework.")


def register_compact_range_method(cls: type, method_name, begin_name, end_name, *arg_types):
    """Register a method which returns a range for Numba calls.

    The implementation uses a pair of begin and end methods which are actually implemented in C++. They must be
    registered before this call.

    Args:
        cls: The class containing the method.
        method_name: The name of the method being registered.
        begin_name: The name of the internal begin method to use.
        end_name: The name of the internal end method to use.
        *arg_types: The argument types to be passed to both begin and end.
    """
    arguments = ", ".join(f"arg{i}" for i in range(len(arg_types)))
    argument_type_checks = "True"
    method_overload_str = f"""
from numba import types
from numba.extending import overload_method

@overload_method(cls._numba_type_wrapper.Type, "{method_name}")
def overload_{begin_name}(self, {arguments}):
    # pylint: disable=unused-argument
    if {argument_type_checks}:

        def impl(self, {arguments}):
            return range(self.{begin_name}({arguments}), self.{end_name}({arguments}))

        return impl
"""
    exec_in_file(f"{cls.__name__}-{method_name}.py", method_overload_str, dict(cls=cls, method_name=method_name))
