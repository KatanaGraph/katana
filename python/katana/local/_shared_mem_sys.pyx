import katana

from katana.cpp.libgalois.Galois cimport SharedMemSys as CSharedMemSys


cdef class SharedMemSys:
    cdef CSharedMemSys katana_runtime


def initialize():
    katana.set_runtime_sys(SharedMemSys)
