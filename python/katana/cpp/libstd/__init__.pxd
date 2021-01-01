# Hack to make auto return type for katana::iterate work.
# It may be necessary to write a wrapper header around for_each,
# but this should be good enough for the forseeable future either way.
cdef extern from * nogil:
    cppclass CPPAuto "auto":
        pass

# Fake types to work around Cython's lack of support
# for non-type template parameters.
cdef extern from *:
    cppclass template_parameter_true "true"
    cppclass template_parameter_false "false"

cdef extern from * nogil:
    # hack to bind leading arguments by value to something that can be passed
    # to for_each. The returned lambda needs to be usable after the scope
    # where it is created closes, so captured values are captured by value.
    # The by-value capture in turn requires that graphs be passed as
    # pointers. This function is used without exception specification under
    # the assumption that it will always be used as a subexpression of
    # a whole expression that requires exception handling or that it will
    # be used in a context where C++ exceptions are appropriate.
    # There are more robust ways to do this, but this didn't require
    # users to find and include additional C++ headers specific to
    # this interface.
    # Syntactically, this is using the cname of an "external" function
    # to create a one-line macro that can be used like a function.
    # The expected use is bind_leading(function, args).
    cdef void *bind_leading "[](auto f, auto&&... bound_args){return [=](auto&&... pars){return f(bound_args..., pars...);};}"(...)
