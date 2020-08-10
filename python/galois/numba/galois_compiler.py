"""
A customized numba compiler pipeline for use with Galois.

This pipeline is used from the `*_operator` decorators in loops.py.
"""
from numba.core import sigutils, compiler
from numba.core.compiler import CompilerBase, DefaultPassBuilder


class GaloisCompiler(CompilerBase):
    """A numba compiler pipeline that is customized for Galois.
    """

    def define_pipelines(self):
        # this maintains the objmode fallback behaviour
        pms = []
        if not self.state.flags.force_pyobject:
            pms.append(DefaultPassBuilder.define_nopython_pipeline(self.state))
        if self.state.status.can_fallback or self.state.flags.force_pyobject:
            pms.append(
                DefaultPassBuilder.define_objectmode_pipeline(self.state)
            )
        if self.state.status.can_giveup:
            pms.append(
                DefaultPassBuilder.define_interpreted_pipeline(self.state)
            )
        return pms


class OperatorCompiler(GaloisCompiler):
    """A numba compiler pipeline that is customized for Galois operators.

    Operators are assumed not to leak references to any memory objects, like arrays.
    """

    def __init__(self, typingctx, targetctx, library, args, return_type, flags, locals):
        flags.nrt = False
        super().__init__(typingctx, targetctx, library, args, return_type, flags, locals)


# TODO: This is duplicated from numba to allow pipeline_class to be passed through. This should be merged upstream.
def cfunc(sig, locals={}, cache=False, pipeline_class=compiler.Compiler, **options):
    """
    This decorator is used to compile a Python function into a C callback
    usable with foreign C libraries.

    Usage::
        @cfunc("float64(float64, float64)", nopython=True, cache=True)
        def add(a, b):
            return a + b

    """
    sig = sigutils.normalize_signature(sig)

    def wrapper(func):
        from numba.core.ccallback import CFunc
        res = CFunc(func, sig, locals=locals, options=options, pipeline_class=pipeline_class)
        if cache:
            res.enable_caching()
        res.compile()
        return res

    return wrapper
