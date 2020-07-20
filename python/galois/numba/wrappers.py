from abc import abstractmethod, ABCMeta

import numba.core.ccallback
import numba.types
from llvmlite import ir
from numba.core import cgutils, imputils
from numba.extending import get_cython_function_address, typeof_impl, overload_method, models, register_model, \
    make_attribute_wrapper, unbox, NativeValue

from .utils import call_raw_function_pointer

import logging

_logger = logging.getLogger(__name__)


def get_cython_function_address_with_defaults(full_function_name, default_module_name, default_function_name):
    module_name = None
    function_name = None
    if full_function_name:
        i = full_function_name.rfind(".")
        if i >= 0:
            module_name = full_function_name[:i]
            function_name = full_function_name[i+1:]
        else:
            function_name = full_function_name
    return get_cython_function_address(module_name or default_module_name, function_name or default_function_name)


class NumbaPointerWrapper(metaclass=ABCMeta):
    def __init__(self, orig_typ, override_module_name=None):
        _logger.debug("NumbaPointerWrapper: %r, %r", orig_typ, override_module_name)
        class Type(numba.types.Type):
            def __init__(self):
                super(Type, self).__init__(name=orig_typ.__name__)

        typ = Type()

        @typeof_impl.register(orig_typ)
        def typeof_(val, c):
            return typ

        @register_model(Type)
        class Model_(models.StructModel):
            def __init__(self, dmm, fe_type):
                members = [('ptr', numba.types.voidptr)]
                models.StructModel.__init__(self, dmm, fe_type, members)

        make_attribute_wrapper(Type, 'ptr', 'ptr')

        @imputils.lower_constant(Type)
        def constant_(context, builder, ty, pyval):
            ptr = ir.Constant(ir.IntType(64), self.get_value_address(pyval)).inttoptr(ir.PointerType(ir.IntType(8)))
            ret = ir.Constant.literal_struct((ptr,))
            return ret

        self.Type = Type
        self.type = typ
        self.type_name = orig_typ.__name__
        self.module_name = orig_typ.__module__
        self.override_module_name = override_module_name or self.module_name
        self.orig_type = orig_typ

    def register_method(self, func_name, typ, cython_func_name=None, addr=None):
        addr_found = get_cython_function_address_with_defaults(
            cython_func_name, self.override_module_name, self.type_name + "_" + func_name)
        if addr:
            assert addr == addr_found
        func = typ(addr_found)
        _logger.debug("%r.register_method: %r, %r: %r%r, %x, %r", self, func_name, func, func.restype, func.argtypes, addr_found, cython_func_name)
        @overload_method(self.Type, func_name)
        def overload(v, *args):
            def impl(v, *args):
                return func(v.ptr, *args)
            return impl
        return overload

    @abstractmethod
    def get_value_address(self, pyval):
        raise NotImplementedError()

    def __repr__(self):
        return "<{} {} {}>".format(type(self).__name__, self.orig_type, self.type)


class SimpleNumbaPointerWrapper(NumbaPointerWrapper):
    def __init__(self, *args):
        super().__init__(*args)

        @unbox(self.Type)
        def unbox_(typ, obj, c):
            ptr_obj = c.pyapi.object_getattr_string(obj, "address")
            ctx = cgutils.create_struct_proxy(typ)(c.context, c.builder)
            ctx.ptr = c.pyapi.long_as_voidptr(ptr_obj)
            c.pyapi.decref(ptr_obj)
            is_error = cgutils.is_not_null(c.builder, c.pyapi.err_occurred())
            return NativeValue(ctx._getvalue(), is_error=is_error)

    def get_value_address(self, pyval):
        return pyval.address


class NativeNumbaPointerWrapper(NumbaPointerWrapper):
    def __init__(self, orig_typ, addr_func, addr_func_name=None, override_module_name=None):
        super().__init__(orig_typ, override_module_name)

        try:
            addr_func_c = get_cython_function_address_with_defaults(
                addr_func_name, self.override_module_name, self.type_name + "_get_address")
        except ValueError:
            addr_func_c = get_cython_function_address_with_defaults(
                addr_func_name, self.override_module_name, self.type_name + "_get_address_c")

        @unbox(self.Type)
        def unbox_(typ, obj, c):
            ctx = cgutils.create_struct_proxy(typ)(c.context, c.builder)
            ctx.ptr = call_raw_function_pointer(
                addr_func_c,
                ir.FunctionType(ir.PointerType(ir.IntType(8)), (ir.PointerType(ir.IntType(8)),)),
                (obj,),
                c)
            return NativeValue(ctx._getvalue())

        self.addr_func = addr_func

    def get_value_address(self, pyval):
        return self.addr_func(pyval)
