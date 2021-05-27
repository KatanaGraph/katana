from .cpp.libgalois.DynamicBitset cimport DynamicBitset as CDynamicBitset


cdef class DynamicBitset:

    cdef CDynamicBitset underlying

    def __cinit__(self):
        self.underlying = CDynamicBitset()

    def resize(self, n):
        """Resizes the bitset."""
        self.underlying.resize(n)

    def reserve(self, n):
        """Reserves capacity for the bitset."""
        self.underlying.reserve(n)

    def clear(self):
        """Clears the bitset."""
        self.underlying.clear()

    def shrink_to_fit(self):
        """Shrinks the allocation for bitset to its current size."""
        self.underlying.shrink_to_fit()

    def size(self):
        """Gets the number of bits held by the bitset."""
        return self.underlying.size()

    def reset(self):
        """Unset every bit in the bitset."""
        self.underlying.reset()

    def reset_begin_end(self, begin, end):
        """Unset a range of bits given an inclusive range."""
        self.underlying.reset(begin, end)

    def test(self, index):
        """Check a bit to see if it is currently set.

        Using this is recommended only if set() and reset()
        are not being used in that parallel section/phase."""
        return self.underlying.test(index)

    def set(self, index):
        """Set a bit in the bitset."""
        return self.underlying.set(index)

    def reset_index(self, index):
        """Reset a bit in the bitset."""
        return self.underlying.reset(index)

    def count(self):
        """Count how many bits are set in the bitset."""
        return self.underlying.count()
