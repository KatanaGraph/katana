from functools import wraps

import numba
import numba.core.ccallback
import numba.types

from ._loops import do_all, for_each, UserContext
from .numba.closure import ClosureBuilder, Closure
from .numba.galois import UserContextType

__all__ = ["do_all", "do_all_operator", "for_each", "for_each_operator", "UserContext"]


###### Parallel loops

do_all_unbound_argument_types = (numba.types.uint64,)

def do_all_operator(typ=None, nopython=True, target="cpu", **kws):
    def decorator(f):
        n_args = f.__code__.co_argcount-1
        f_jit = numba.jit(typ, nopython=nopython, target=target, **kws)(f)
        builder = wraps(f)(ClosureBuilder(f_jit, do_all_unbound_argument_types, target=target))
        if n_args == 0:
            return builder()
        else:
            return builder
    return decorator

def is_do_all_operator_cfunc(v):
    try:
        return isinstance(v, numba.core.ccallback.CFunc) and v.__wrapped__.__code__.co_argcount == 2
    except AttributeError:
        return False

def is_do_all_operator_closure(v):
    return isinstance(v, Closure) and v.unbound_argument_types == do_all_unbound_argument_types

for_each_unbound_argument_types = (numba.types.uint64, UserContextType.type)

def for_each_operator(typ=None, nopython=True, target="cpu", **kws):
    def decorator(f):
        n_args = f.__code__.co_argcount-2
        f_jit = numba.jit(typ, nopython=nopython, target=target, **kws)(f)
        builder = wraps(f)(ClosureBuilder(f_jit, for_each_unbound_argument_types, target=target))
        if n_args == 0:
            return builder()
        else:
            return builder
    return decorator

def is_for_each_operator_cfunc(v):
    try:
        return isinstance(v, numba.core.ccallback.CFunc) and v.__wrapped__.__code__.co_argcount == 3
    except AttributeError:
        return False

def is_for_each_operator_closure(v):
    return isinstance(v, Closure) and v.unbound_argument_types == for_each_unbound_argument_types
