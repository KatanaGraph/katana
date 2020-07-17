import ctypes
import operator

import numba.core.ccallback
import numba.types
import pyarrow
from llvmlite import ir
from numba.core import cgutils, imputils
from numba.extending import get_cython_function_address, typeof_impl, overload, overload_method, models, register_model, \
    make_attribute_wrapper, unbox, NativeValue

from . import _pyarrow_wrappers
from .utils import call_raw_function_pointer
from .wrappers import NativeNumbaPointerWrapper, get_cython_function_address_with_defaults

__all__ = []


###### Wrap typed Arrow arrays for Numba

class ArrowArrayNumbaPointerWrapper(NativeNumbaPointerWrapper):
    def __init__(self, orig_typ, addr_func, element_type, addr_func_name=None, override_module_name=None):
        super().__init__(orig_typ, addr_func, addr_func_name, override_module_name)

        assert self.type_name.endswith("Array")
        element_type_name = self.type_name[:-5]

        addr = get_cython_function_address(
            self.override_module_name, "Array_length")
        Array_length = ctypes.CFUNCTYPE(ctypes.c_uint64, ctypes.c_voidp)(addr)

        @overload(len)
        def overload_len(v):
            if isinstance(v, self.Type):
                def impl_(v):
                    return Array_length(v.ptr)
                return impl_

        addr = get_cython_function_address(
            self.override_module_name, "Array_is_valid")
        Array_is_valid = ctypes.CFUNCTYPE(ctypes.c_bool, ctypes.c_voidp, ctypes.c_uint64)(addr)

        @overload_method(self.Type, "is_valid")
        def overload_is_valid(v, i):
            def impl_(v, i):
                return Array_is_valid(v.ptr, i)
            return impl_

        @overload_method(self.Type, "is_null")
        def overload_is_null(v, i):
            def impl_(v, i):
                return not Array_is_valid(v.ptr, i)
            return impl_

        @overload_method(self.Type, "indicies")
        def overload_indicies(v):
            def impl_(v):
                for i in range(Array_length(v.ptr)):
                    if v.is_valid(i):
                        yield i
            return impl_

        addr = get_cython_function_address(
            self.override_module_name, "Array_" + element_type_name + "Array_Value")
        Array_xArray_Value = ctypes.CFUNCTYPE(element_type, ctypes.c_voidp, ctypes.c_uint64)(addr)

        @overload(operator.getitem)
        def overload_getitem(v, i):
            if isinstance(v, self.Type):
                def impl_(v, i):
                    return Array_xArray_Value(v.ptr, i)
                return impl_

        @overload_method(self.Type, "values")
        def overload_values(v):
            def impl_(v):
                for i in range(Array_length(v.ptr)):
                    if v.is_valid(i):
                        yield Array_xArray_Value(v.ptr, i)
            return impl_

        # # TODO: This doesn't actually work. There doesn't seem to be any way to overload iter or __iter__.
        # @overload(iter)
        # def overload_iter(v):
        #     if isinstance(v, self.Type):
        #         def impl_(v):
        #             for i in range(Array_length(v.ptr)):
        #                 yield Array_xArray_Value(v.ptr, i)
        #         return impl_


ArrowArrayNumbaPointerWrapper(pyarrow.Int64Array, _pyarrow_wrappers.Array_get_address,
                              ctypes.c_int64,
                              addr_func_name="Array_get_address_c",
                              override_module_name="galois.numba._pyarrow_wrappers")
ArrowArrayNumbaPointerWrapper(pyarrow.Int32Array, _pyarrow_wrappers.Array_get_address,
                              ctypes.c_int32,
                              addr_func_name="Array_get_address_c",
                              override_module_name="galois.numba._pyarrow_wrappers")
ArrowArrayNumbaPointerWrapper(pyarrow.UInt64Array, _pyarrow_wrappers.Array_get_address,
                              ctypes.c_uint64,
                              addr_func_name="Array_get_address_c",
                              override_module_name="galois.numba._pyarrow_wrappers")
ArrowArrayNumbaPointerWrapper(pyarrow.UInt32Array, _pyarrow_wrappers.Array_get_address,
                              ctypes.c_uint32,
                              addr_func_name="Array_get_address_c",
                              override_module_name="galois.numba._pyarrow_wrappers")
ArrowArrayNumbaPointerWrapper(pyarrow.lib.FloatArray, _pyarrow_wrappers.Array_get_address,
                              ctypes.c_float,
                              addr_func_name="Array_get_address_c",
                              override_module_name="galois.numba._pyarrow_wrappers")
ArrowArrayNumbaPointerWrapper(pyarrow.lib.DoubleArray, _pyarrow_wrappers.Array_get_address,
                              ctypes.c_double,
                              addr_func_name="Array_get_address_c",
                              override_module_name="galois.numba._pyarrow_wrappers")


###### Wrap chunked Arrow arrays for Numba

_array_type_map = {
    pyarrow.int64(): pyarrow.Int64Array,
    pyarrow.int32(): pyarrow.Int32Array,
    pyarrow.uint64(): pyarrow.UInt64Array,
    pyarrow.uint32(): pyarrow.UInt32Array,
    pyarrow.float64(): pyarrow.lib.DoubleArray,
    pyarrow.float32(): pyarrow.lib.FloatArray
    }

_type_array_map = {a: t for t, a in _array_type_map.items()}

_arrow_ctypes_map = {
    pyarrow.int64(): ctypes.c_int64,
    pyarrow.int32(): ctypes.c_int32,
    pyarrow.uint64(): ctypes.c_uint64,
    pyarrow.uint32(): ctypes.c_uint32,
    pyarrow.float64(): ctypes.c_double,
    pyarrow.float32(): ctypes.c_float
    }

class ChunkedArrayNumbaPointerWrapper(NativeNumbaPointerWrapper):
    def __init__(self, orig_typ, addr_func, addr_func_name=None, override_module_name=None):
        # HACK: No super call

        class Type(numba.types.Type):
            def __init__(self, chunk_type):
                super(Type, self).__init__(name=orig_typ.__name__ + "[" + chunk_type.__name__ + "]")
                self.chunk_type = chunk_type

            @property
            def key(self):
                return (self.name, self.chunk_type)

        typs = {k: Type(c) for k, c in _array_type_map.items()}

        @typeof_impl.register(orig_typ)
        def typeof_(val, c):
            return typs[val.type]

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
        self.types = typs
        self.type_name = orig_typ.__name__
        self.module_name = orig_typ.__module__
        self.override_module_name = override_module_name or self.module_name
        self.orig_type = orig_typ

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


        addr = get_cython_function_address(self.override_module_name, "ChunkedArray_length")
        ChunkedArray_length = ctypes.CFUNCTYPE(ctypes.c_uint64, ctypes.c_voidp)(addr)

        @overload(len)
        def overload_len(v):
            if isinstance(v, self.Type):
                def impl_(v):
                    return ChunkedArray_length(v.ptr)
                return impl_

        addr = get_cython_function_address(self.override_module_name, "ChunkedArray_num_chunks")
        ChunkedArray_num_chunks = ctypes.CFUNCTYPE(ctypes.c_uint64, ctypes.c_voidp)(addr)

        addr = get_cython_function_address(self.override_module_name, "ChunkedArray_Array_chunk_length")
        ChunkedArray_Array_chunk_length = ctypes.CFUNCTYPE(ctypes.c_uint64, ctypes.c_voidp, ctypes.c_uint64)(addr)

        @overload_method(self.Type, "_convert_index")
        def overload_getitem(v, i):
            if isinstance(v, self.Type):
                ChunkedArray_xArray_Value = ChunkedArray_xArray_Value_map[v]
                if isinstance(i, numba.types.UniTuple) and i.types == (numba.types.uint64, numba.types.uint64):
                    def impl_(v, i):
                        return i
                    return impl_
                else:
                    def impl_(v, i):
                        chunk = 0
                        while True: # contains break
                            chunk_len = ChunkedArray_Array_chunk_length(v.ptr, chunk)
                            if i < chunk_len:
                                break
                            i -= chunk_len
                            chunk += 1
                        return (chunk, i)
                    return impl_

        addr = get_cython_function_address(
            self.override_module_name, "ChunkedArray_Array_is_valid")
        ChunkedArray_is_valid = ctypes.CFUNCTYPE(ctypes.c_bool, ctypes.c_voidp, ctypes.c_uint64, ctypes.c_uint64)(addr)

        @overload_method(self.Type, "is_valid")
        def overload_is_valid(v, ind):
            def impl_(v, ind):
                c, i = v._convert_index(ind)
                return ChunkedArray_is_valid(v.ptr, c, i)
            return impl_

        @overload_method(self.Type, "is_null")
        def overload_is_null(v, ind):
            def impl_(v, ind):
                c, i = v._convert_index(ind)
                return not ChunkedArray_is_valid(v.ptr, c, i)
            return impl_

        @overload_method(self.Type, "indicies")
        def overload_indicies(v):
            def impl_(v):
                for c in range(ChunkedArray_num_chunks(v.ptr)):
                    for i in range(ChunkedArray_Array_chunk_length(v.ptr, c)):
                        if v.is_valid((c, i)):
                            yield (c, i)
            return impl_


        def get_chunked_array_xarray_value(element_type, element_type_name):
            addr = get_cython_function_address(
                self.override_module_name, "ChunkedArray_" + element_type_name + "Array_Value")
            ChunkedArray_xArray_Value = ctypes.CFUNCTYPE(element_type, ctypes.c_voidp, ctypes.c_uint64, ctypes.c_uint64)(addr)
            return ChunkedArray_xArray_Value
        ChunkedArray_xArray_Value_map = {
            t: get_chunked_array_xarray_value(_arrow_ctypes_map[_type_array_map[t.chunk_type]], t.chunk_type.__name__[:-5])
            for t in typs.values()
            }

        @overload(operator.getitem)
        def overload_getitem(v, i):
            if isinstance(v, self.Type):
                ChunkedArray_xArray_Value = ChunkedArray_xArray_Value_map[v]
                if isinstance(i, numba.types.UniTuple) and i.types == (numba.types.uint64, numba.types.uint64):
                    def impl_(v, i):
                        return ChunkedArray_xArray_Value(v.ptr, i[0], i[1])
                    return impl_
                else:
                    def impl_(v, i):
                        chunk = 0
                        while True: # contains break
                            chunk_len = ChunkedArray_Array_chunk_length(v.ptr, chunk)
                            if i < chunk_len:
                                break
                            i -= chunk_len
                            chunk += 1
                        return ChunkedArray_xArray_Value(v.ptr, chunk, i)
                    return impl_

        @overload_method(self.Type, "values")
        def overload_values(v):
            ChunkedArray_xArray_Value = ChunkedArray_xArray_Value_map[v]
            def impl_(v):
                for c in range(ChunkedArray_num_chunks(v.ptr)):
                    for i in range(ChunkedArray_Array_chunk_length(v.ptr, c)):
                        if v.is_valid((c, i)):
                            yield ChunkedArray_xArray_Value(v.ptr, c, i)
            return impl_

        # # TODO: This doesn't actually work. There doesn't seem to be any way to overload iter or __iter__.
        # @overload(iter)
        # def overload_iter(v):
        #     if isinstance(v, self.Type):
        #         def impl_(v):
        #             for i in range(Array_length(v.ptr)):
        #                 yield Array_xArray_Value(v.ptr, i)
        #         return impl_

# ChunkedArrayType = NativeNumbaPointerWrapper(
#     pyarrow.ChunkedArray, _pyarrow_wrappers.ChunkedArray_get_address,
#     override_module_name="galois.numba._pyarrow_wrappers")
ChunkedArrayNumbaPointerWrapper(
    pyarrow.ChunkedArray, _pyarrow_wrappers.ChunkedArray_get_address,
    override_module_name="galois.numba._pyarrow_wrappers")
