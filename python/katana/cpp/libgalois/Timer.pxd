from libc.stdint cimport uint64_t


cdef extern from "katana/Timer.h" namespace "katana" nogil:
    cppclass Timer:
        Timer()
        void start()
        void stop()
        uint64_t get()
        uint64_t get_usec()

    cppclass TimeAccumulator:
        TimeAccumulator()
        void start()
        void stop()
        uint64_t get()
        uint64_t get_usec()

    cppclass StatTimer(TimeAccumulator):
        StatTimer(const char* name, const char* region)
