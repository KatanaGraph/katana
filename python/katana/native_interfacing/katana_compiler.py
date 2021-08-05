"""
A customized numba compiler pipeline for use with Galois.

This pipeline is used from the `*_operator` decorators in loop_operators.py.
"""

import numba.cpython.builtins
from llvmlite import ir
from numba.core import compiler, sigutils, types
from numba.core.compiler import CompilerBase, DefaultPassBuilder
from numba.core.imputils import lower_constant

disable_nrt = True
external_function_pointer_as_constant = True


class KatanaCompiler(CompilerBase):
    """A numba compiler pipeline that is customized for Galois.
    """

    def define_pipelines(self):
        # this maintains the objmode fallback behaviour
        pms = []
        if not self.state.flags.force_pyobject:
            pms.append(DefaultPassBuilder.define_nopython_pipeline(self.state))
        if self.state.status.can_fallback or self.state.flags.force_pyobject:
            pms.append(DefaultPassBuilder.define_objectmode_pipeline(self.state))
        if hasattr(self.state.status, "can_giveup") and self.state.status.can_giveup:
            pms.append(DefaultPassBuilder.define_interpreted_pipeline(self.state))
        return pms


class OperatorCompiler(KatanaCompiler):
    """A numba compiler pipeline that is customized for Galois operators.

    Operators are assumed not to leak references to any memory objects, like arrays.
    """

    def __init__(self, typingctx, targetctx, library, args, return_type, flags, local_vars):
        if disable_nrt:
            flags.nrt = False
        super().__init__(typingctx, targetctx, library, args, return_type, flags, local_vars)
        targetctx.is_operator_context = True


@lower_constant(types.ExternalFunctionPointer)
def constant_function_pointer(context, builder: ir.IRBuilder, ty, pyval):
    """
    Override the internal handling of ExternalFunctionPointer to avoid generating a global variable.
    """
    if (
        external_function_pointer_as_constant
        and hasattr(context, "is_operator_context")
        and context.is_operator_context
    ):
        ptrty = context.get_function_pointer_type(ty)
        return ir.Constant(ir.types.IntType(64), ty.get_pointer(pyval)).inttoptr(ptrty)
    # If we are not in an operator context (as defined by OperatorCompiler use) then call the numba implementation
    return numba.cpython.builtins.constant_function_pointer(context, builder, ty, pyval)


# TODO: This is duplicated from numba to allow pipeline_class to be passed through. This should be merged upstream.
def cfunc(sig, local_vars=None, cache=False, pipeline_class=compiler.Compiler, **options):
    """
    This decorator is used to compile a Python function into a C callback
    usable with foreign C libraries.

    Usage::
        @cfunc("float64(float64, float64)", nopython=True, cache=True)
        def add(a, b):
            return a + b

    """
    sig = sigutils.normalize_signature(sig)
    local_vars = local_vars or {}

    def wrapper(func):
        from numba.core.ccallback import CFunc

        res = CFunc(func, sig, locals=local_vars, options=options, pipeline_class=pipeline_class)
        if cache:
            res.enable_caching()
        res.compile()
        return res

    return wrapper
