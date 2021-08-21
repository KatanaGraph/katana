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
