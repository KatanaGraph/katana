from .cpp.libgalois.Galois cimport getVersion as c_getVersion
from .cpp.libgalois.Galois cimport setActiveThreads as c_setActiveThreads

_katana_runtime = _katana_runtime_wrapper()

def set_active_threads(int n):
    return c_setActiveThreads(n)

def get_version():
    return str(c_getVersion(), encoding="ASCII")
