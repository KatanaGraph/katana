from enum import Enum


class Architecture(Enum):
    CPU = _Architecture.kCPU
    GPU = _Architecture.kGPU
    Distributed = _Architecture.kDistributed


cdef class Plan:
    cdef _Plan* underlying(self) except NULL:
        raise NotImplementedError()

    def architecture(self) -> Architecture:
        return Architecture(self.underlying().architecture())
