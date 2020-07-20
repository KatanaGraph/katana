import ctypes
from functools import wraps

import numba
from numba import types, typeof
from numba.core import cgutils
from numba.extending import lower_builtin
from numba.extending import make_attribute_wrapper
from numba.extending import models, register_model
from numba.extending import typeof_impl, type_callable

from galois.numba._native_wrapper_utils import call_callback


# TODO: This whole setup is a mess and almost certainly leaks all references passed to closures.
#  This should be replaced ASAP.

class Closure():
    def __init__(self, func, userdata, unbound_argument_types):
        self._function = func
        self._userdata = userdata
        self.unbound_argument_types = unbound_argument_types

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


class _ClosureInstance():
    def __init__(self, func, bound_args, unbound_args, target):
        # TODO: Only wrapper depends on func. The other stuff could be cached based on only bound_args (globally)
        class Environment():
            pass

        class EnvironmentType(types.Type):
            def __init__(self):
                super(EnvironmentType, self).__init__(name='Environment')

        environment_type = EnvironmentType()

        @typeof_impl.register(Environment)
        def typeof_environment(val, c):
            return environment_type

        @register_model(EnvironmentType)
        class EnvironmentModel(models.StructModel):
            def __init__(self, dmm, fe_type):
                members = [("arg" + str(i), t) for i, t in enumerate(bound_args)]
                models.StructModel.__init__(self, dmm, fe_type, members)

        for i, t in enumerate(bound_args):
            name = "arg" + str(i)
            make_attribute_wrapper(EnvironmentType, name, name)

        def cast_to_Environment(p):
            raise NotImplementedError("Not callable from Python")

        @type_callable(cast_to_Environment)
        def type_environment(context):
            def typer(v):
                if isinstance(v, types.Integer):
                    return types.CPointer(environment_type)
            return typer

        @lower_builtin(cast_to_Environment, types.uint64)
        def impl_environment(context, builder, sig, args):
            return builder.inttoptr(args[0], context.get_value_type(sig.return_type))

        exec_glbls = locals()
        exec_glbls["type_callable"] = type_callable
        exec_glbls["lower_builtin"] = lower_builtin
        exec_glbls["numba"] = numba
        exec_glbls["types"] = types
        exec_glbls["cgutils"] = cgutils

        assign_env = "; ".join(f"environment.arg{i} = args[{i}]" for i, t in enumerate(bound_args))
        env_args = ", ".join(f"arg{i}" for i, t in enumerate(bound_args))
        env_args_tuple = "()" if not bound_args else "(" + ", ".join(f"arg{i}" for i, t in enumerate(bound_args)) + ",)"
        unbound_pass_args = "" if not unbound_args else ", ".join(f"unbound_arg{i}" for i, t in enumerate(unbound_args)) + ","
        extract_env = "" if not bound_args else ", ".join(f"userdata.arg{i}" for i, t in enumerate(bound_args)) + ","
        src = f"""
@type_callable(Environment)
def type_environment(context):
    def typer({env_args}):
        if {env_args_tuple} == tuple(bound_args):
            return environment_type
    return typer

@lower_builtin(Environment, *bound_args)
def impl_environment(context, builder, sig, args):
    if context.enable_nrt:
        for argty, argval in zip(sig.args, args):
            context.nrt.incref(builder, argty, argval)
            context.nrt.incref(builder, argty, argval) # WTF?!?!?! Why do I need to incref twice?
    environment = cgutils.create_struct_proxy(sig.return_type)(context, builder)
    {assign_env}
    return environment._getvalue()

@numba.jit(types.void(types.uint64, *bound_args), nopython=True, target=target)
def fill(userdata, {env_args}):
    userdata_ptr = cast_to_Environment(userdata)
    numba.carray(userdata_ptr, 1)[0] = Environment({env_args})

@numba.cfunc(types.void(*unbound_args, types.CPointer(environment_type)), nopython=True, nogil=True, cache=False)
def wrapper({unbound_pass_args} userdata):
    userdata = numba.carray(userdata, 1)[0]
    func({extract_env} {unbound_pass_args})
"""
        # print(bound_args, unbound_args)
        # print(src)
        exec(src, exec_glbls)

        assert target == "cpu", "Only cpu is supported as target because numba.cfunc only supports cpu."

        self.fill = exec_glbls["fill"]
        self.wrapper = wraps(func)(exec_glbls["wrapper"])
        self.environment_type = environment_type
        self.target = target
        self.environment_size = self._get_environment_size()

    def _get_environment_size(self):
        context = numba.core.registry.dispatcher_registry[self.target].targetdescr.target_context
        return context.get_abi_sizeof(context.get_value_type(self.environment_type))


# FIXME: Fixed unbound argument type at construction time. Needs to support setting at closure *call* time.
class ClosureBuilder():
    def __init__(self, func, unbound_argument_types, target="cpu"):
        self._underlying_function = func
        self._instance_cache = {}
        self._target = target
        self._unbound_argument_types = tuple(unbound_argument_types)

    def _generate(self, bound_args):
        key = bound_args
        if key in self._instance_cache:
            return self._instance_cache[key]
        else:
            inst = _ClosureInstance(self._underlying_function, bound_args, unbound_args=self._unbound_argument_types, target=self._target)
            self._instance_cache[key] = inst
            return inst

    def __call__(self, *args):
        arg_types = tuple(typeof(v) for v in args)
        inst = self._generate(arg_types)
        env = (ctypes.c_byte * inst.environment_size)()
        env_ptr = ctypes.addressof(env)
        # print(env_ptr, *args)
        inst.fill(env_ptr, *args)
        closure = Closure(inst.wrapper, env, self._unbound_argument_types)
        closure._EnvironmentStruct_arguments = args
        return closure

    def __str__(self):
        return "<Closure builder {} {}>".format(self._underlying_function, self._unbound_argument_types)
