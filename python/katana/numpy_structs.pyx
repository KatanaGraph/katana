import numpy as np
cimport numpy as np


cdef np.ndarray argument_to_ndarray_dtype(v, np.dtype dtype):
    cdef np.ndarray arr
    if not isinstance(v, np.ndarray):
        arr = np.array([v], dtype=dtype)
    else:
        arr = v
    return arr


cdef object as_struct_instance(np.ndarray raw, np.dtype dtype):
    # TODO: This does a lot of conversions. All are zero copy and this is called from interpreted python. But
    #  simplifying would be good anyway.
    return raw.view(np.int8)[:dtype.itemsize].view(dtype).view(StructInstance)


class StructInstance(np.ndarray):
    def __new__(cls, *args, dtype):
        self = super(StructInstance, cls).__new__(cls, shape=(1,), dtype=dtype)
        self._setitem(args)
        return self

    def __array_finalize__(self, obj):
        assert self.shape == (1,)
        assert self.ndim == 1

    def _getitem(self):
        return super(StructInstance, self).__getitem__(0)

    def _setitem(self, v):
        return super(StructInstance, self).__setitem__(0, v)

    def __getattr__(self, item):
        try:
            return self._getitem()[item]
        except ValueError as e:
            raise AttributeError(*e.args)

    def __setattr__(self, item, v):
        self._getitem()[item] = v

    def __getitem__(self, item):
        return self._getitem()[item]

    def __setitem__(self, item, v):
        self._getitem()[item] = v

    def __str__(self):
        return str(self._getitem())

    def __repr__(self):
        return repr(self._getitem())
