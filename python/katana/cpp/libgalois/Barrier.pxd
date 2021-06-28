from libcpp.memory cimport unique_ptr


cdef extern from "katana/Barrier.h" namespace "katana" nogil:
    cppclass Barrier:
        void Reinit(unsigned val)
        void Wait()
        const char* name() const

    Barrier& GetBarrier(unsigned active_threads)
    unique_ptr[Barrier] CreateSimpleBarrier(unsigned active_threads);
