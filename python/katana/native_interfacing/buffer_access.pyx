import numpy
import pyarrow

from cpython.buffer cimport PyBUF_CONTIG, PyBuffer_Release, PyObject_CheckBuffer, PyObject_GetBuffer


cdef class UntypedBufferAccess:
    def __init__(self, data):
        if not PyObject_CheckBuffer(data):
            if hasattr(data, "to_numpy"):
                # Handle pyarrow.Array
                data = data.to_numpy()
            else:
                raise TypeError("data must support buffer protocol")

        if hasattr(data, "dtype"):
            self._numpy_dtype = data.dtype
        else:
            raise TypeError("data must provide a numpy dtype attribute")

        PyObject_GetBuffer(data, &self.buffer, PyBUF_CONTIG)

    def __dealloc__(self):
        PyBuffer_Release(&self.buffer)

    def empty_like(self, *args, **kwds):
        return numpy.empty(*args, dtype=self._numpy_dtype, **kwds)


cpdef to_numpy(v):
    """
    Convert a value to numpy using it's own conversion methods if possible.

    This tries four different ways to convert:

    1. If ``v`` is a numpy array already, return it.
    2. If ``v`` has a method ``to_numpy``, call it and return the result. (supports pyarrow, pandas, etc.)
    3. If ``v`` has a method ``as_numpy``, call it and return the result. (supports tensorflow, etc.)
    3. Call `numpy.array` and return the result.

    In all cases, this function attempts to avoid a copy, but does not guarantee it. The result should be treated as
    immutable in most cases.
    """
    if isinstance(v, numpy.ndarray):
        return v
    if hasattr(v, "to_numpy"):
        return v.to_numpy()
    if hasattr(v, "as_numpy"):
        return v.as_numpy()
    return numpy.array(v, copy=False)


cpdef to_pyarrow(v):
    """
    Convert an array to pyarrow.

    This tries two different ways to convert:

    1. If ``v`` is a pyarrow array already, return it.
    3. Call `pyarrow.array` and return the result.
    """
    if isinstance(v, pyarrow.Array):
        return v
    return pyarrow.array(v)
