from libc.stdint cimport uint32_t


cdef extern from "tsuba/RDGStorageFormatVersion.h" namespace "tsuba" nogil:
    cdef uint32_t kLatestPartitionStorageFormatVersion
