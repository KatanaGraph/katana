from .cpp.libgalois.DynamicBitset cimport DynamicBitset as CDynamicBitset


cdef class DynamicBitset:
    cdef CDynamicBitset underlying

    def __init__(self, num_bits):
        """Initializes the bitset and sets its size to the given number of bits."""
        self.resize(num_bits)

    def resize(self, num_bits):
        """Resizes the bitset."""
        self.underlying.resize(num_bits)

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

    def __getitem__(self, key):
        """Checks a bit to see if it is currently set.

        This is not thread-safe if concurrent with setting bits."""
        if not isinstance(key, int):
            raise TypeError('key has to be an integer')

        # TODO: support slices
        if key < 0 or key >= len(self):
            raise IndexError(key)

        return self.underlying.test(key)

    def __setitem__(self, key, value):
        """Sets or resets a bit in the bitset."""
        # TODO: Ideally we should use the range based reset on the
        # underlying type in cases where it can do what we need. That
        # will save a lot of bit twiddling. A similar set operation
        # could also be added to the underlying type at some
        # point. Setting ranges of bits will be way faster with an
        # operation that knows the in memory representation of the
        # type since we can easily set at least 32-bits at a time.
        if isinstance(key, int):
            indices = slice(key, key + 1, 1)
        elif isinstance(key, slice):
            if key.step != None and key.step > 1:
                raise ValueError(f'slice step can only be 1')
            indices = key
        else:
            raise TypeError('Incorrect key type (should be int or slice)')

        start = 0 if indices.start == None else indices.start
        for index in range(start, indices.stop, 1):
            if index < 0 or index >= len(self):
                raise IndexError(f'{key} out of range')

            if not value:
                self.underlying.reset(index)
            else:
                self.underlying.set(index)

    def count(self):
        """Counts how many bits are set in the bitset."""
        return self.underlying.count()
