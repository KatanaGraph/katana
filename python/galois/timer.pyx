from .cpp.libgalois.Timer cimport Timer as CTimer, StatTimer as CStatTimer

cdef class Timer:
    cdef CTimer underlying

    def start(self):
        self.underlying.start()

    def stop(self):
        self.underlying.stop()

    def get_sec(self):
        return <double>self.underlying.get_usec() / 1000 / 1000 / 1000

    def get(self):
        return <double>self.underlying.get_usec() / 1000

    def get_usec(self):
        return self.underlying.get_usec()


cdef class StatTimer:
    cdef CStatTimer* underlying
    cdef object name, region

    def __init__(self, name = None, region = None):
        self.name = bytes(name, "utf-8") if name else b"Time"
        self.region = bytes(region, "utf-8") if region else b"(NULL)"
        self.underlying = NULL
        self._init()

    def __dealloc__(self):
        self._fini()

    cdef _init(self):
        if self.underlying == NULL:
            self.underlying = new CStatTimer(self.name, self.region)

    cdef _fini(self):
        if self.underlying != NULL:
            del self.underlying
            self.underlying = NULL

    cdef _check(self):
        if self.underlying == NULL:
            raise RuntimeError("StatTimer is already finalized")

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.stop()

    def finalize(self):
        self._fini()

    def start(self):
        self._check()
        self.underlying.start()

    def stop(self):
        self._check()
        self.underlying.stop()

    def get_sec(self):
        self._check()
        return <double>self.underlying.get_usec() / 1000 / 1000 / 1000

    def get(self):
        self._check()
        return <double>self.underlying.get_usec() / 1000

    def get_usec(self):
        self._check()
        return self.underlying.get_usec()
