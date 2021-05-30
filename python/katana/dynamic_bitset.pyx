from .cpp.libgalois.DynamicBitset cimport DynamicBitset as CDynamicBitset


cdef class DynamicBitset:
    cdef CDynamicBitset underlying

    def __cinit__(self):
        self.underlying = CDynamicBitset()

    def resize(self, n):
        """Resizes the bitset."""
        self.underlying.resize(n)

    def clear(self):
        """Resets all bits and sets the size of the set to 0."""
        self.underlying.clear()

    def shrink_to_fit(self):
        """Shrinks the allocation for bitset to its current size."""
        self.underlying.shrink_to_fit()

    def __len__(self):
        """Gets the number of bits held by the bitset."""
        return self.underlying.size()

    def reset(self):
        """Unsets every bit in the bitset."""
        self.underlying.reset()

    def __getitem__(self, index):
        """Checks a bit to see if it is currently set.

        This is not safe w.r.t. setting bits."""
        if not isinstance(index, int):
            raise TypeError('index has to be an integer')

        if index < 0 or index >= len(self):
            raise IndexError(f'{index} out of range')

        return self.underlying.test(index)

    def __setitem__(self, key, value):
        """Sets or resets a bit in the bitset."""
        if isinstance(key, int):
            indices = slice(key, key + 1, 1)
        elif isinstance(key, slice):
            if key.step != None and key.step > 1:
                raise AttributeError(f'slice step can only be 1')
            indices = key
        else:
            raise TypeError('Incorrect key type (should be int or slice)')

        start = 0 if indices.start == None else indices.start
        for index in list(range(start, indices.stop, 1)):
            if index < 0 or index >= len(self):
                raise IndexError(f'{key} out of range')

            if value == 0:
                self.underlying.reset(index)
            elif value == 1:
                self.underlying.set(index)
            else:
                raise AttributeError(f'{value} is invalid value (should be 0 or 1)')

    def count(self):
        """Counts how many bits are set in the bitset."""
        return self.underlying.count()
