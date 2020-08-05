import ctypes
from functools import wraps

import llvmlite.ir
from galois.numba._native_wrapper_utils import call_callback
from numba import types, typeof, njit
from numba.experimental import jitclass
from numba.extending import lower_builtin
from numba.extending import type_callable

from galois.numba.galois_compiler import OperatorCompiler, cfunc

PointerPair = ctypes.c_void_p * 2


class Closure:
    """
    A closure containing a native function pointer and the environment needed to invoke it. These closures are
    used by galois to specify operators.
    """

    def __init__(self, func, userdata, return_type, unbound_argument_types, captured=()):
        """
        :param func: The function to of this closure. Must have an address attribute returning a function pointer as an int.
        :param userdata: The userdata pointer to be passed to `func` as a ctypes value.
        :param return_type: The numba return type of the function.
        :param unbound_argument_types: The numba types of the unbound arguments of the function.
        :param captured: A tuple of values that must be accessible to prevent the userdata from being deallocated.
        """
        self._function = func
        self._userdata = userdata
        self.return_type = return_type
        self.unbound_argument_types = unbound_argument_types
        self._captured = captured

    @property
    def __function_address__(self):
        return self._function.address

    @property
    def __userdata_address__(self):
        return ctypes.addressof(self._userdata)

    def __call__(self, arg):
        return call_callback(self.__function_address__, arg, self.__userdata_address__)

    def __str__(self):
        return "<Closure {} {}>".format(self._function, self._userdata)


class _ClosureInstance:
    """
    A closure structure and wrapper compiled for specific types.
    """

    def __init__(self, func, return_type, bound_args, unbound_args, target):
        Environment = self._build_environment(bound_args)
        store_struct = self._build_store_struct(Environment)
        load_struct = self._build_load_struct(Environment)
        wrapper = self._build_wrapper(func, load_struct, return_type, bound_args, unbound_args)

        assert target == "cpu", "Only cpu is supported as target because numba.cfunc only supports cpu."

        self.Environment = Environment
        self.store_struct = store_struct
        self.wrapper = wraps(func)(wrapper)
        self.target = target

    def _build_environment(self, bound_args):
        """
        Construct a numba jitclass structure with elements named arg1 ... argn with types bound_args[1...n].
        """
        spec = [("arg" + str(i), t) for i, t in enumerate(bound_args)]
        exec_glbls = locals()
        exec_glbls["jitclass"] = jitclass
        assign_env = "; ".join(f"self.arg{i} = arg{i}" for i, t in enumerate(bound_args))
        env_args = ", ".join(f"arg{i}" for i, t in enumerate(bound_args))
        src = f"""
@jitclass(spec)
class Environment():
    def __init__(self, {env_args}):
        {assign_env}
"""
        exec(src, exec_glbls)
        return exec_glbls["Environment"]

    def _build_wrapper(self, func, load_struct, return_type, bound_args, unbound_args):
        """
        The arguments are unpacked from the jitclass pointer passed as an int64.
        """
        exec_glbls = locals()
        exec_glbls["cfunc"] = cfunc
        exec_glbls["types"] = types
        exec_glbls["OperatorCompiler"] = OperatorCompiler
        unbound_pass_args = (
            "" if not unbound_args else ", ".join(f"unbound_arg{i}" for i, t in enumerate(unbound_args)) + ","
        )
        extract_env = "" if not bound_args else ", ".join(f"userdata.arg{i}" for i, t in enumerate(bound_args)) + ","
        src = f"""
@cfunc(return_type(*unbound_args, types.int64), nopython=True, nogil=True, cache=False, pipeline_class=OperatorCompiler)
def wrapper({unbound_pass_args} userdata):
    userdata = load_struct(userdata)
    return func({extract_env} {unbound_pass_args})
"""
        exec(src, exec_glbls)
        return exec_glbls["wrapper"]

    def _build_load_struct(self, Environment):
        """
        Construct a numba builtin function which takes a pointer (passed as int64) and loads a jitclass from it.
        """

        def load_struct(t):
            raise NotImplementedError("Not callable from Python")

        @type_callable(load_struct)
        def type_load_struct(context):
            def typer(t):
                if isinstance(t, types.Integer):
                    return Environment.class_type.instance_type

            return typer

        @lower_builtin(load_struct, types.int64)
        def impl_load_struct(context, builder: llvmlite.ir.IRBuilder, sig, args):
            struct_ty = context.get_value_type(sig.return_type)
            ptr = builder.inttoptr(args[0], struct_ty.as_pointer())
            struct = builder.load(ptr)
            # We incref here since this function should give away a reference.
            if context.enable_nrt:
                context.nrt.incref(builder, sig.return_type, struct)
            return struct

        return load_struct

    def _build_store_struct(self, Environment):
        """
        Construct a python function which takes a jitclass instance and a pointer (passed as int64) and copies the
        jitclass into the pointer. This is implemented using a jit function and a numba builtin. The buffer must be
        two pointers in size.
        """

        def store_struct(s, t):
            raise NotImplementedError("Not callable from Python")

        @type_callable(store_struct)
        def type_store_struct(context):
            def typer(s, t):
                if s == Environment.class_type.instance_type and isinstance(t, types.Integer):
                    return types.void

            return typer

        @lower_builtin(store_struct, Environment.class_type.instance_type, types.int64)
        def impl_store_struct(context, builder: llvmlite.ir.IRBuilder, sig, args):
            struct_ty = context.get_value_type(sig.args[0])
            ptr = builder.inttoptr(args[1], struct_ty.as_pointer())
            builder.store(args[0], ptr)

        @njit
        def store_struct_py(s, t):
            store_struct(s, t)

        return store_struct_py


# FIXME: Fixed unbound argument type at construction time. Needs to support setting at closure *call* time.
class ClosureBuilder:
    """
    A factory object for closures.

    This object manages a cache of compiled closures structures and wrappers (as _ClosureInstance objects).
    """

    def __init__(self, func, *, return_type=types.void, unbound_argument_types, target="cpu"):
        self._underlying_function = func
        self._instance_cache = {}
        self._target = target
        self._unbound_argument_types = tuple(unbound_argument_types)
        self._return_type = return_type

    def _generate(self, bound_args):
        key = bound_args
        if key in self._instance_cache:
            return self._instance_cache[key]
        else:
            inst = _ClosureInstance(
                self._underlying_function,
                self._return_type,
                bound_args,
                unbound_args=self._unbound_argument_types,
                target=self._target,
            )
            self._instance_cache[key] = inst
            return inst

    def __call__(self, *args):
        arg_types = tuple(typeof(v) for v in args)
        inst = self._generate(arg_types)
        env = PointerPair()
        env_ptr = ctypes.addressof(env)
        env_struct = inst.Environment(*args)
        inst.store_struct(env_struct, env_ptr)
        closure = Closure(
            inst.wrapper, env, self._return_type, self._unbound_argument_types, captured=(env_struct, args),
        )
        return closure

    def __str__(self):
        return "<Closure builder {} {}>".format(self._underlying_function, self._unbound_argument_types)

    def inspect_llvm(self):
        return {types: inst.wrapper.inspect_llvm() for types, inst in self._instance_cache.items()}
