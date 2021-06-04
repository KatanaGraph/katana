cdef extern from "katana/analytics/Plan.h" namespace "katana::analytics" nogil:
    enum _Architecture "katana::analytics::Architecture":
        kCPU
        kGPU
        kDistributedCPU
        kDistributedGPU

    cppclass _Plan "katana::analytics::Plan":
        _Architecture architecture() const


cdef class Plan:
    cdef _Plan* underlying(self) except NULL


cdef class Statistics:
    pass
