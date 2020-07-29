from .cpp.libgalois.Timer cimport Timer as CTimer, StatTimer as CStatTimer

cdef class Timer:
    """
    A simple high-resolution performance timer.

    The timer may be stopped and started with the `start()` and `stop()` methods or by using the timer as a context manager:

    >>> timer = Timer()
    ... with timer: ...
    """
    cdef CTimer underlying

    def start(self):
        """
        start(self)

        Start the timer.
        """
        self.underlying.start()

    def stop(self):
        """
        stop(self)

        Stop the timer.
        """
        self.underlying.stop()

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.stop()

    def get_sec(self):
        """
        get_sec(self)

        Return the accumulated time in seconds (as a float).
        """
        return <double>self.underlying.get_usec() / 1000 / 1000 / 1000

    def get(self):
        """
        get(self)

        Return the accumulated time in milliseconds (as a float).
        """
        return <double>self.underlying.get_usec() / 1000

    def get_usec(self):
        """
        get_usec(self)

        Return the accumulated time in microseconds (as an int).
        """
        return self.underlying.get_usec()


cdef class StatTimer:
    """
    A high-resolution performance timer which is reported to Galois' performance counter registry.

    The timer may be stopped and started with the `start()` and `stop()` methods or by using the timer as a context manager:

    >>> timer = StatTimer("Test", "Inner")
    ... with timer: ...

    The time is only reported to Galois when this object is destoyed so the user should make sure this object goes out
    of scope before the program exits or that they call call `finalize()`.
    """
    cdef CStatTimer* underlying
    cdef object name, region

    def __init__(self, name = None, region = None):
        """
        __init__(self, name: str = None, region: str = None)

        Construct a time with the specified name and region.
        """
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
        """
        finalize(self)

        Report the timer to the Galois performance registry and destroy the timer.
        After this is called this object cannot be used any more.
        """
        self._fini()

    def start(self):
        """
        start(self)

        Start the timer.
        """
        self._check()
        self.underlying.start()

    def stop(self):
        """
        stop(self)

        Stop the timer.
        """
        self._check()
        self.underlying.stop()

    def get_sec(self):
        """
        get_sec(self)

        Return the accumulated time in seconds (as a float).
        """
        self._check()
        return <double>self.underlying.get_usec() / 1000 / 1000 / 1000

    def get(self):
        """
        get(self)

        Return the accumulated time in milliseconds (as a float).
        """
        self._check()
        return <double>self.underlying.get_usec() / 1000

    def get_usec(self):
        """
        get_usec(self)

        Return the accumulated time in microseconds (as an int).
        """
        self._check()
        return self.underlying.get_usec()
