from libc.stdint cimport uint32_t


cdef extern from "katana/RDGStorageFormatVersion.h" namespace "katana" nogil:
    cdef uint32_t kLatestPartitionStorageFormatVersion
