from katana.cpp.libgalois.Galois cimport GetThreadPool
from katana.cpp.libgalois.Galois cimport getActiveThreads as c_getActiveTheads
from katana.cpp.libgalois.Galois cimport getVersion as c_getVersion
from katana.cpp.libgalois.Galois cimport setActiveThreads as c_setActiveThreads

__all__ = ["get_active_threads", "set_active_threads", "set_busy_wait", "get_version"]


def get_active_threads():
    """
    :return: The number of threads Katana is configured to use for work.
    """
    return c_getActiveTheads()


def set_active_threads(int n):
    """
    Set the number of threads Katana should use to do computation.

    :return: The number of threads actually used which may be less than `n`
    """
    return c_setActiveThreads(n)


def set_busy_wait(n = None):
    """
    Configure Katana threads busy wait for work. This decreases latency at the cost of 100% CPU usage even if there is
    no work to be done.

    :type n: int or None
    :param n: The number of cores to busy wait on, or None to busy wait on all active Katana threads. If `n` is 0 then
        this will disable busy waiting.
    """
    if n is None:
        n = get_active_threads()
    return GetThreadPool().burnPower(<int>n)


def get_version():
    return str(c_getVersion(), encoding="ASCII")
