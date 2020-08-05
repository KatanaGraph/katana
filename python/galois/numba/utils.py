from llvmlite import ir


def call_raw_function_pointer(func_ptr, function_type, args, builder: ir.IRBuilder):
    val = ir.Constant(ir.IntType(64), func_ptr)
    ptr = builder.inttoptr(val, ir.PointerType(function_type))
    # Due to limitations in llvmlite ptr cannot be a constant, so do the cast as an instruction to make the call
    # argument an instruction.
    return builder.call(ptr, args)


def interpret_numba_wrapper_tables(tables, globals=None):
    from galois.numba.wrappers import SimpleNumbaPointerWrapper

    for typ, table in tables:
        assert (
            hasattr(typ, "address") and hasattr(typ.address, "__get__") and not hasattr(typ.address, "__call__")
        ), "{}.address does not exist or is not a property.".format(typ)
        Type = SimpleNumbaPointerWrapper(typ)
        interpret_numba_wrapper_table(Type, table)
        if globals:
            globals[typ.__name__ + "_numba_wrapper"] = Type
            globals[typ.__name__ + "_numba_type"] = Type.type


def interpret_numba_wrapper_table(Type, table):
    for name, func_type, impl_func_name, addr in table:
        Type.register_method(name, func_type, impl_func_name, addr=addr)


def _dump_llvm(f, output_func):
    d = f.inspect_llvm()
    if isinstance(d, dict):
        for ty, code in d.items():
            output_func("\n===== {}\n{}".format(ty, code))
    else:
        output_func("\n" + d)


def dump_numba_llvm(func):
    out = print
    module = __import__(func.__module__)
    if hasattr(module, "_logger"):
        out = module._logger.debug
    if hasattr(func, "inspect_llvm"):
        _dump_llvm(func, output_func=out)
