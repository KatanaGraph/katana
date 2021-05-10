import logging
from abc import abstractmethod, ABCMeta
from functools import lru_cache
from typing import Union

import numpy as np
import numba.core.ccallback
import numba.types
from llvmlite import ir
from numba import from_dtype
from numba.core import cgutils, imputils
from numba.core.base import BaseContext
from numba.extending import (
    get_cython_function_address,
    typeof_impl,
    overload_method,
    models,
    register_model,
    make_attribute_wrapper,
    unbox,
    NativeValue,
    type_callable,
    lower_builtin,
)

from ..util import wraps_class
from ..util.template_type import find_size_for_dtype

_logger = logging.getLogger(__name__)


def get_cython_function_address_with_defaults(full_function_name, default_module_name, default_function_name):
    module_name = None
    function_name = None
    if full_function_name:
        i = full_function_name.rfind(".")
        if i >= 0:
            module_name = full_function_name[:i]
            function_name = full_function_name[i + 1 :]
        else:
            function_name = full_function_name
    return get_cython_function_address(module_name or default_module_name, function_name or default_function_name)


class NumbaPointerWrapper(metaclass=ABCMeta):
    def __init__(self, orig_typ, override_module_name=None):
        _logger.debug("NumbaPointerWrapper: %r, %r", orig_typ, override_module_name)
        Type = self._build_typing(orig_typ)

        self._build_model(Type)

        self.Type = Type
        self.type_name = orig_typ.__name__
        self.module_name = orig_typ.__module__
        self.override_module_name = override_module_name or self.module_name
        self.orig_type = orig_typ

    def _build_model(self, Type):
        @register_model(Type)
        class Model(models.StructModel):
            def __init__(self, dmm, fe_type):
                members = [("ptr", numba.types.voidptr)]
                models.StructModel.__init__(self, dmm, fe_type, members)
        _ = Model

        make_attribute_wrapper(Type, "ptr", "ptr")

        @imputils.lower_constant(Type)
        def constant(context, builder, ty, pyval):
            _ = context
            _ = builder
            _ = ty
            ptr = ir.Constant(ir.IntType(64), self.get_value_address(pyval)).inttoptr(ir.PointerType(ir.IntType(8)))
            ret = ir.Constant.literal_struct((ptr,))
            return ret

        _ = constant

    def _build_typing(self, orig_typ):
        @wraps_class(orig_typ, "<numba type>")
        class Type(numba.types.Type):
            def __init__(self):
                super().__init__(name=orig_typ.__name__)

        @typeof_impl.register(orig_typ)
        def typeof(val, c):
            _ = val
            _ = c
            return Type()

        _ = typeof

        return Type

    def register_method(self, func_name, typ, cython_func_name=None, addr=None, dtype_arguments=None):
        addr_found = get_cython_function_address_with_defaults(
            cython_func_name, self.override_module_name, self.type_name + "_" + func_name,
        )
        if addr:
            assert addr == addr_found
        func = typ(addr_found)

        if dtype_arguments is None:
            dtype_arguments = [False] * len(func.argtypes)
        _logger.debug(
            "%r.register_method: %r, %r: %r%r, %x, %r",
            self,
            func_name,
            func,
            func.restype,
            func.argtypes,
            addr_found,
            cython_func_name,
        )
        exec_glbls = dict(self=self, func_name=func_name, func=func)
        exec_glbls["overload_method"] = overload_method
        exec_glbls["construct_dtype_on_stack"] = construct_dtype_on_stack
        arguments = ", ".join(f"arg{i}" for i, _ in enumerate(dtype_arguments))
        arguments_construct = ", ".join(
            f"construct_dtype_on_stack(self, arg{i})" if is_dtype else f"arg{i}"
            for i, is_dtype in enumerate(dtype_arguments)
        )
        src = f"""
@overload_method(self.Type, func_name)
def overload(self, {arguments}):
    def impl(self, {arguments}):
        return func(self.ptr, {arguments_construct})
    return impl
"""
        exec(src, exec_glbls)
        return exec_glbls["overload"]

    @abstractmethod
    def get_value_address(self, pyval):
        raise NotImplementedError()

    def __repr__(self):
        return "<{} {} {}>".format(type(self).__name__, self.orig_type, self.Type)


class SimpleNumbaPointerWrapper(NumbaPointerWrapper):
    def __init__(self, orig_typ, override_module_name=None):
        assert (
            hasattr(orig_typ, "address")
            and hasattr(orig_typ.address, "__get__")
            and not hasattr(orig_typ.address, "__call__")
        ), "{}.address does not exist or is not a property.".format(orig_typ)
        super().__init__(orig_typ, override_module_name)

        @unbox(self.Type)
        def unbox_(typ, obj, c):
            ptr_obj = c.pyapi.object_getattr_string(obj, "address")
            ctx = cgutils.create_struct_proxy(typ)(c.context, c.builder)
            ctx.ptr = c.pyapi.long_as_voidptr(ptr_obj)
            c.pyapi.decref(ptr_obj)
            is_error = cgutils.is_not_null(c.builder, c.pyapi.err_occurred())
            return NativeValue(ctx._getvalue(), is_error=is_error)

        _ = unbox_

    def get_value_address(self, pyval):
        return pyval.address


class DtypeParametricType(numba.types.Type):
    def __init__(self, name, dtype):
        super().__init__(name=name)
        if not isinstance(dtype, np.dtype):
            raise TypeError("dtype must be a dtype: " + str(dtype))
        self.dtype = dtype

    @property
    def key(self):
        return self.name, self.dtype

    @property
    def mangling_args(self):
        typ = self.dtype_as_type()
        if isinstance(typ, numba.types.Record):
            return self.name, tuple(t for _, t in typ.members)
        return self.name, (typ,)

    @lru_cache(1)
    def dtype_as_type(self) -> Union[numba.types.Record, numba.types.Type]:
        return from_dtype(self.dtype)


class DtypeNumbaPointerWrapper(SimpleNumbaPointerWrapper):
    def __init__(self, orig_typ, override_module_name=None):
        super().__init__(orig_typ, override_module_name)
        # TODO: Is there a way to check for ".dtype"? Probably not, it's an attribute and we don't have an instance.

    def _build_typing(self, orig_typ):
        @wraps_class(orig_typ, "<numba type>")
        class Type(DtypeParametricType):
            def __init__(self, dtype):
                super().__init__(dtype=dtype, name=orig_typ.__name__)

        @typeof_impl.register(orig_typ)
        def typeof_(val, c):
            _ = c
            return Type(val.dtype)

        _ = typeof_

        return Type


class NativeNumbaPointerWrapper(NumbaPointerWrapper):
    def __init__(self, orig_typ, addr_func, addr_func_name=None, override_module_name=None):
        super().__init__(orig_typ, override_module_name)
        self.addr_func = self._build_unbox_by_call(addr_func, addr_func_name)

    def _build_unbox_by_call(self, addr_func, addr_func_name):
        try:
            addr_func_c = get_cython_function_address_with_defaults(
                addr_func_name, self.override_module_name, self.type_name + "_get_address",
            )
        except ValueError:
            addr_func_c = get_cython_function_address_with_defaults(
                addr_func_name, self.override_module_name, self.type_name + "_get_address_c",
            )

        @unbox(self.Type)
        def unbox_type(typ, obj, c):
            ctx = cgutils.create_struct_proxy(typ)(c.context, c.builder)
            ctx.ptr = call_raw_function_pointer(
                addr_func_c,
                ir.FunctionType(ir.PointerType(ir.IntType(8)), (ir.PointerType(ir.IntType(8)),)),
                (obj,),
                c.builder,
            )
            return NativeValue(ctx._getvalue())

        _ = unbox_type

        return addr_func

    def get_value_address(self, pyval):
        return self.addr_func(pyval)


def construct_dtype_on_stack(self, values):
    """
    (Numba compiled only) Return a stack allocated instance of the self.dtype (self must be a DtypeParametricType) with
    the field values taken from the tuple `values`.
    """
    raise NotImplementedError("Not callable from Python")


@type_callable(construct_dtype_on_stack)
def type_construct_dtype_on_stack(context):
    _ = context
    def typer(self, values):
        if isinstance(self, DtypeParametricType) and isinstance(values, numba.types.BaseTuple):
            return numba.types.voidptr
        return None

    return typer


@lower_builtin(construct_dtype_on_stack, DtypeParametricType, numba.types.BaseTuple)
def impl_construct_dtype_on_stack(context: BaseContext, builder: ir.IRBuilder, sig, args):
    ty = sig.args[0].dtype_as_type()
    containing_size = find_size_for_dtype(sig.args[0].dtype)
    ptr = builder.alloca(ir.IntType(8), containing_size)
    for i, (name, mem_ty) in enumerate(ty.members):
        llvm_mem_ty = context.get_value_type(mem_ty)
        offset = ty.offset(name)
        v = builder.extract_value(args[1], i)
        v = context.cast(builder, v, sig.args[1][i], mem_ty)
        v_ptr_byte = builder.gep(ptr, (ir.Constant(ir.IntType(32), offset),), True)
        v_ptr = builder.bitcast(v_ptr_byte, llvm_mem_ty.as_pointer())
        builder.store(v, v_ptr)
    return ptr


def call_raw_function_pointer(func_ptr, function_type, args, builder: ir.IRBuilder):
    val = ir.Constant(ir.IntType(64), func_ptr)
    ptr = builder.inttoptr(val, ir.PointerType(function_type))
    # Due to limitations in llvmlite ptr cannot be a constant, so do the cast as an instruction to make the call
    # argument an instruction.
    return builder.call(ptr, args)


def interpret_numba_wrapper_tables(tables, global_vars=None):
    for typ, with_dtype, table in tables:
        if with_dtype:
            Type = DtypeNumbaPointerWrapper(typ)
        else:
            Type = SimpleNumbaPointerWrapper(typ)
        interpret_numba_wrapper_table(Type, table)
        if global_vars:
            global_vars[typ.__name__ + "_numba_wrapper"] = Type
            global_vars[typ.__name__ + "_numba_type"] = Type.Type


def interpret_numba_wrapper_table(Type, table):
    for name, func_type, impl_func_name, addr, dtype_arguments in table:
        Type.register_method(name, func_type, impl_func_name, addr=addr, dtype_arguments=dtype_arguments)
