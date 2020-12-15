from libcpp.string cimport string

cdef extern from "<system_error>" namespace "std" nogil:
    cdef cppclass error_category:
        const char* name()

    cdef cppclass error_code:
        int value()
        string message()
        error_category category()

cdef extern from "<boost/outcome/outcome.hpp>" namespace "BOOST_OUTCOME_V2_NAMESPACE" nogil:
    cdef cppclass std_result[T]:
        T value()
        bint has_value()
        bint has_failure()
        error_code error()

cdef inline void raise_error_code(error_code err) except *:
    # Importing error_category_to_exception_class directly into this module does not work due to how cython importing works.
    import galois

    category_name = str(err.category().name(), "ascii")
    exception_type = galois.error_category_to_exception_class.get(category_name, RuntimeError)
    if category_name in galois.error_category_to_exception_class:
        prefix = ""
    else:
        prefix = category_name + ": "
    raise exception_type(prefix + str(err.message(), "ascii"))

cdef inline int handle_result_void(std_result[void] res) nogil except 0:
    if not res.has_value():
        with gil:
            raise_error_code(res.error())
    return 1

