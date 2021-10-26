import warnings
from functools import wraps
from threading import Lock

import numba
from numba import types
from numba.core import cgutils
from numba.core.typing.arraydecl import get_array_index_type
from numba.extending import lower_builtin, type_callable
from numba.np.arrayobj import basic_indexing, make_array, normalize_indices

__all__ = ["atomic_add", "atomic_sub", "atomic_max", "atomic_min"]


def atomic_rmw(context, builder, op, arrayty, val, ptr):
    assert arrayty.aligned  # We probably have to have aligned arrays.
    dataval = context.get_value_as_data(builder, arrayty.dtype, val)
    return builder.atomic_rmw(op, ptr, dataval, "monotonic")


# The global lock used to protect atomic operations when called from python code in DISABLE_JIT mode.
_global_atomics_lock = Lock()


if numba.config.DISABLE_JIT:
    warnings.warn(
        "Atomic operations are not fully atomic when DISABLE_JIT is set. Only use DISABLE_JIT for testing "
        "and debugging."
    )


def declare_atomic_array_op(iop, uop, fop):
    def decorator(func):
        @type_callable(func)
        def func_type(context):
            def typer(ary, idx, val):
                out = get_array_index_type(ary, idx)
                if out is not None:
                    res = out.result
                    if context.can_convert(val, res):
                        return res
                return None

            return typer

        _ = func_type

        @lower_builtin(func, types.Buffer, types.Any, types.Any)
        def func_impl(context, builder, sig, args):
            """
            array[a] = scalar_or_array
            array[a,..,b] = scalar_or_array
            """
            aryty, idxty, valty = sig.args
            ary, idx, val = args

            if isinstance(idxty, types.BaseTuple):
                index_types = idxty.types
                indices = cgutils.unpack_tuple(builder, idx, count=len(idxty))
            else:
                index_types = (idxty,)
                indices = (idx,)

            ary = make_array(aryty)(context, builder, ary)

            # First try basic indexing to see if a single array location is denoted.
            index_types, indices = normalize_indices(context, builder, index_types, indices)
            dataptr, shapes, _strides = basic_indexing(
                context, builder, aryty, ary, index_types, indices, boundscheck=context.enable_boundscheck,
            )
            if shapes:
                raise NotImplementedError("Complex shapes are not supported")

            # Store source value the given location
            val = context.cast(builder, val, valty, aryty.dtype)
            op = None
            if isinstance(aryty.dtype, types.Integer) and aryty.dtype.signed:
                op = iop
            elif isinstance(aryty.dtype, types.Integer) and not aryty.dtype.signed:
                op = uop
            elif isinstance(aryty.dtype, types.Float):
                op = fop
            if op is None:
                raise TypeError("Atomic operation not supported on " + str(aryty))
            return atomic_rmw(context, builder, op, aryty, val, dataptr)

        _ = func_impl

        @wraps(func)
        def wrapper(*args, **kwargs):
            with _global_atomics_lock:
                func(*args, **kwargs)

        return wrapper

    return decorator


@declare_atomic_array_op("add", "add", "fadd")
def atomic_add(ary, i, v):
    """
    Atomically, perform `ary[i] += v` and return the previous value of `ary[i]`.

    i must be a simple index for a single element of ary. Broadcasting and vector operations are not supported.

    This should be used from numba compiled code.
    """
    orig = ary[i]
    ary[i] += v
    return orig


@declare_atomic_array_op("sub", "sub", "fsub")
def atomic_sub(ary, i, v):
    """
    Atomically, perform `ary[i] -= v` and return the previous value of `ary[i]`.

    i must be a simple index for a single element of ary. Broadcasting and vector operations are not supported.

    This should be used from numba compiled code.
    """
    orig = ary[i]
    ary[i] -= v
    return orig


@declare_atomic_array_op("max", "umax", None)
def atomic_max(ary, i, v):
    """
    Atomically, perform `ary[i] = max(ary[i], v)` and return the previous value of `ary[i]`.
    This operation does not support floating-point values.

    i must be a simple index for a single element of ary. Broadcasting and vector operations are not supported.

    This should be used from numba compiled code.
    """
    orig = ary[i]
    ary[i] = max(ary[i], v)
    return orig


@declare_atomic_array_op("min", "umin", None)
def atomic_min(ary, i, v):
    """
    Atomically, perform `ary[i] = min(ary[i], v)` and return the previous value of `ary[i]`.
    This operation does not support floating-point values.

    i must be a simple index for a single element of ary. Broadcasting and vector operations are not supported.

    This should be used from numba compiled code.
    """
    orig = ary[i]
    ary[i] = min(ary[i], v)
    return orig
