from .cpp.libgalois.Galois cimport setActiveThreads as c_setActiveThreads
from .cpp.libgalois.Galois cimport getVersion as c_getVersion


_katana_runtime = _katana_runtime_wrapper()

def setActiveThreads(int n):
    return c_setActiveThreads(n)

def get_version():
    return c_getVersion()

