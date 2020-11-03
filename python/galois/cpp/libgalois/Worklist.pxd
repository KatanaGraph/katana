cdef extern from "galois/Traits.h" namespace "galois" nogil:
    cppclass s_wl:
        pass
    s_wl wl[T](...)

cdef extern from "galois/worklists/Chunk.h" namespace "galois::worklists" nogil:
    cppclass ChunkFIFO[T]:
        pass
    cppclass PerSocketChunkFIFO[T]:
        pass

cdef extern from "galois/worklists/Obim.h" namespace "galois::worklists" nogil:
    cppclass OrderedByIntegerMetric[UpdateFuncTy, WorkListTy]:
        pass


        