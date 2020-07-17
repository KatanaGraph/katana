from llvmlite import ir

def call_raw_function_pointer(func_ptr, function_type, args, c): 
    function_type = ir.FunctionType(ir.PointerType(ir.IntType(8)),
                                    (ir.PointerType(ir.IntType(8)),))
    ptr = ir.Constant(ir.IntType(64), func_ptr).inttoptr(ir.PointerType(function_type))
    # HACK: Add a field to ptr which is expected by builder.call based on the
    #  assumption that the function is a normal Function.
    ptr.function_type = function_type
    return c.builder.call(ptr, args)

