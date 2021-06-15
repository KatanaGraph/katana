import numpy

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
