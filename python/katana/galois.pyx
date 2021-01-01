from .cpp.libgalois.Galois cimport setActiveThreads as c_setActiveThreads

_katana_runtime = _katana_runtime_wrapper()

def setActiveThreads(int n):
    return c_setActiveThreads(n)

