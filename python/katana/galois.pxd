from .cpp.libgalois.Galois cimport SharedMemSys

# Initialize the Galois runtime when the Python module is loaded.
cdef class _katana_runtime_wrapper:
    cdef SharedMemSys _katana_runtime
