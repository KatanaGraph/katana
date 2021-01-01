cdef extern from "galois/Bag.h" namespace "galois" nogil:
    cppclass InsertBag[T]:
        cppclass iterator:
            T& operator*()
            iterator operator++()
            iterator operator--()
            bint operator==(iterator)
            bint operator!=(iterator)

        void push(T)
        bint empty()
        void swap(InsertBag&)
        void clear()

        iterator begin()
        iterator end()

cdef extern from "galois/LargeArray.h" namespace "galois" nogil:
    cppclass LargeArray[T]:
        cppclass iterator:
            T& operator*()
            iterator operator++()
            iterator operator--()
            bint operator==(iterator)
            bint operator!=(iterator)

        void allocateInterleaved(size_t)
        void allocateBlocked(size_t)
        T &operator[](size_t)
        void set(size_t, const T&)

        size_t size()

        iterator begin()
        iterator end()

        T* data()
