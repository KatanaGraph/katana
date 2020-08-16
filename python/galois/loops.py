from functools import wraps

import numba
import numba.core.ccallback
import numba.types

from ._loops import (
    do_all,
    for_each,
    UserContext_numba_type,
    OrderedByIntegerMetric,
    UserContext,
    PerSocketChunkFIFO,
)
from .numba.closure import ClosureBuilder, Closure
from .numba.galois_compiler import OperatorCompiler

__all__ = [
    "do_all",
    "do_all_operator",
    "for_each",
    "for_each_operator",
    "obim_metric",
    "OrderedByIntegerMetric",
    "UserContext",
    "PerSocketChunkFIFO",
]


# Parallel loops

# TODO: Hard coded uint64_t loop variable

do_all_unbound_argument_types = (numba.types.uint64,)


def do_all_operator(typ=None, nopython=True, target="cpu", **kws):
    """
    >>> @do_all_operator()
    ... def f(arg0, ..., argn, element): ...

    Decorator to declare an operator for use with a `do_all`.
    The operators have some restructions; see below.
    If the operator has any arguments other than the element argument expected from the loop, those arguments must be
    bound by calling the function to create a closure:

    >>> f(arg0, ..., argn)

    The operator is compiled using numba.
    Its argument types are inferred automatically based on the binding call.
    Multiple uses of the same operator with same type will reuse the same cached compiled copy of the function.

    Operators have some restrictions:

    * The operator may not create new references to arrays or other dynamically allocated values. For example, an
      operator may not add a numpy array to a global list.
    """

    def decorator(f):
        n_args = f.__code__.co_argcount - 1
        f_jit = numba.jit(typ, nopython=nopython, target=target, pipeline_class=OperatorCompiler, **kws)(f)
        builder = wraps(f)(ClosureBuilder(f_jit, unbound_argument_types=do_all_unbound_argument_types, target=target,))
        if n_args == 0:
            return builder()
        return builder

    return decorator


def is_do_all_operator_cfunc(v):
    try:
        return isinstance(v, numba.core.ccallback.CFunc) and v.__wrapped__.__code__.co_argcount == 2
    except AttributeError:
        return False


def is_do_all_operator_closure(v):
    return isinstance(v, Closure) and v.unbound_argument_types == do_all_unbound_argument_types


# TODO: Hard coded uint64_t loop variable
for_each_unbound_argument_types = (numba.types.uint64, UserContext_numba_type)


def for_each_operator(typ=None, nopython=True, target="cpu", **kws):
    """
    >>> @for_each_operator()
    ... def f(arg0, ..., argn, element, ctx): ...

    Decorator to declare an operator for use with a `do_all`.
    The operators have some restructions; see below.
    If the operator has any arguments other than the element and context arguments expected from the loop, those
    arguments must be bound by calling the function to create a closure:

    >>> f(arg0, ..., argn)

    The operator is compiled using numba.
    Its argument types are inferred automatically based on the binding call.
    Multiple uses of the same operator with same type will reuse the same cached compiled copy of the function.

    Operators have some restrictions:

    * The operator may not create new references to arrays or other dynamically allocated values. For example, an
      operator may not add a numpy array to a global list.
    """

    def decorator(f):
        n_args = f.__code__.co_argcount - 2
        f_jit = numba.jit(typ, nopython=nopython, target=target, pipeline_class=OperatorCompiler, **kws)(f)
        builder = wraps(f)(
            ClosureBuilder(f_jit, unbound_argument_types=for_each_unbound_argument_types, target=target,)
        )
        if n_args == 0:
            return builder()
        return builder

    return decorator


def is_for_each_operator_cfunc(v):
    try:
        return isinstance(v, numba.core.ccallback.CFunc) and v.__wrapped__.__code__.co_argcount == 3
    except AttributeError:
        return False


def is_for_each_operator_closure(v):
    return isinstance(v, Closure) and v.unbound_argument_types == for_each_unbound_argument_types


# Ordered By Integer Metric


def obim_metric(typ=None, nopython=True, target="cpu", **kws):
    """
    >>> @obim_metric()
    ... def f(arg0, ..., argn, element): ...

    Decorator to declare a metric for use with `OrderedByIntegerMetric`.

    Metrics have some restrictions:

    * The metric may not create new references to arrays or other dynamically allocated values. For example, a
      metric may not add a numpy array to a global list.
    """

    def decorator(f):
        n_args = f.__code__.co_argcount - 1
        f_jit = numba.jit(typ, nopython=nopython, target=target, pipeline_class=OperatorCompiler, **kws)(f)
        builder = wraps(f)(
            ClosureBuilder(
                f_jit,
                return_type=numba.types.int64,
                unbound_argument_types=do_all_unbound_argument_types,
                target=target,
            )
        )
        if n_args == 0:
            return builder()
        return builder

    return decorator


def is_obim_metric_cfunc(v):
    try:
        return isinstance(v, numba.core.ccallback.CFunc) and v.__wrapped__.__code__.co_argcount == 3
    except AttributeError:
        return False


def is_obim_metric_closure(v):
    return (
        isinstance(v, Closure)
        and v.return_type == numba.types.int64
        and v.unbound_argument_types == do_all_unbound_argument_types
    )


# Import the numba wrappers people are likely to need.
# TODO: This imports should probably be elsewhere, but this will work for now.
import galois.numba.galois
import galois.numba.pyarrow
