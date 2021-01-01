cimport numpy as np

cdef np.ndarray argument_to_ndarray_dtype(v, np.dtype dtype)

cdef object as_struct_instance(np.ndarray raw, np.dtype dtype)
