from libcpp cimport bool
from libcpp.string cimport string

from katana.cpp.libstd.iostream cimport ostream, ostringstream
from katana.cpp.libstd.system_error cimport error_category, error_code


cdef extern from "katana/Result.h" namespace "katana" nogil:
    cdef cppclass ErrorInfo:
        error_code error_code()
        ostream& Write(ostream&)


cdef extern from "katana/Result.h" namespace "katana" nogil:
    cdef cppclass Result[T]:
        T value()
        bint has_value()
        bint has_failure()
        ErrorInfo error()


cdef inline void raise_error_code(ErrorInfo err) except *:
    cdef ostringstream out
    err.Write(out)

    # Importing error_category_to_exception_class directly into this module does not work due to how cython importing works.
    import katana

    category_name = str(err.error_code().category().name(), "ascii")
    exception_type = katana.error_category_to_exception_class.get(category_name, RuntimeError)
    raise exception_type(str(out.str(), "ascii"))


cdef inline int handle_result_void(Result[void] res) nogil except 0:
    if not res.has_value():
        with gil:
            raise_error_code(res.error())
    return 1


cdef inline bool handle_result_bool(Result[bool] res) nogil except? False:
    # Using "except? False" will make for some spurious exception checks, however amp doesn't see how to avoid this
    # without requiring manual handling at every call site. We are already transitioning to Python so it shouldn't
    # matter.
    if not res.has_value():
        with gil:
            raise_error_code(res.error())
    return res.value()


cdef inline int handle_result_assert(Result[void] res) nogil except 0:
    cdef ostringstream out

    if not res.has_value():
        with gil:
            err = res.error()
            err.Write(out)

            raise AssertionError(str(out.str(), "ascii"))
    return 1
