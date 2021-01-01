cdef extern from "katana/Traits.h" namespace "katana" nogil:
    cppclass s_wl:
        pass
    s_wl wl[T](...)

cdef extern from "katana/Chunk.h" namespace "katana" nogil:
    cppclass ChunkFIFO[T]:
        pass
    cppclass PerSocketChunkFIFO[T]:
        pass

cdef extern from "katana/Obim.h" namespace "katana" nogil:
    cppclass OrderedByIntegerMetric[UpdateFuncTy, WorkListTy]:
        pass
