from katana.cpp.libstd.atomic cimport atomic


cdef extern from "katana/Reduction.h" namespace "katana" nogil:
    cppclass Reducible[T]:
        void update(T&&)
        void update_const "update"(const T&)
        void reset()
        T& reduce()
        T& getLocal()

    cppclass GAccumulator[T](Reducible[T]):
        pass

    cppclass GReduceMax[T](Reducible[T]):
        pass

    cppclass GReduceMin[T](Reducible[T]):
        pass

    cppclass GReduceLogicalAnd(Reducible[bint]):
        pass

    cppclass GReduceLogicalOr(Reducible[bint]):
        pass

cdef extern from "katana/AtomicHelpers.h" namespace "katana" nogil:
    const T atomicMin[T](atomic[T]&, const T)
    const T atomicMax[T](atomic[T]&, const T)
