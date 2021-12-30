import ctypes

from katana.native_interfacing.wrappers import SimpleNumbaPointerWrapper


def _simplify_type(t):
    if t is None:
        return None
    if t.__module__ == "ctypes":
        return t
    return ctypes.c_void_p


def register_method(cls, method, invoker_data, invoker_ptr, ret_type, *arg_types):
    numba_wrapper: SimpleNumbaPointerWrapper = cls._numba_wrapper
    ctypes_func_type = ctypes.CFUNCTYPE(
        _simplify_type(ret_type), ctypes.c_int64, _simplify_type(cls), *(_simplify_type(t) for t in arg_types)
    )
    numba_wrapper.register_method(method.__name__, ctypes_func_type, addr=invoker_ptr, data=invoker_data)


def register_function(func, func_ptr, ret_type, *arg_types):
    print(func, func.__name__)
    print(func_ptr)
    print(ret_type, arg_types)
    raise NotImplementedError()
