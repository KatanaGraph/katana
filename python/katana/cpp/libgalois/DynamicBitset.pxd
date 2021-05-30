from libcpp cimport bool


cdef extern from "katana/DynamicBitset.h" namespace "katana" nogil:

    cdef cppclass DynamicBitset:
        DynamicBitset()
        void resize(size_t n)
        void clear()
        void shrink_to_fit()
        size_t size()
        void reset()
        void reset(size_t begin, size_t end)
        bool test(size_t index)
        bool set(size_t index)
        bool reset(size_t index)
        size_t count()
