from libc.stdint cimport uint8_t


cdef class UntypedBufferAccess:
    cdef Py_buffer buffer
    cdef object _numpy_dtype

    cdef inline uint8_t* ptr(self):
        return <uint8_t *> self.buffer.buf

    cdef inline Py_ssize_t itemsize(self):
        return self.buffer.itemsize

    cdef inline Py_ssize_t len(self):
        return self.buffer.len

    cdef inline object dtype(self):
        return self._numpy_dtype


cpdef to_numpy(v)
cpdef to_pyarrow(v)
