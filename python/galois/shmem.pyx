from .cpp.libgalois.Galois cimport setActiveThreads as c_setActiveThreads

_galois_runtime = _galois_runtime_wrapper()

def setActiveThreads(int n):
    return c_setActiveThreads(n)

