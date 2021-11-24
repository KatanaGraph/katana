from libcpp.string cimport string

from ..libstd cimport CPPAuto


cdef extern from "katana/ThreadPool.h" namespace "katana" nogil:
    cppclass ThreadPool:
        void burnPower(unsigned num)
        void beKind()

    ThreadPool& GetThreadPool()


cdef extern from "katana/SharedMemSys.h" namespace "katana" nogil:
    unsigned int setActiveThreads(unsigned int)
    unsigned int getActiveThreads()

    cppclass UserContext[T]:
        void push(...)
        void push_back(...)
        bint isFirstPass()
        void cautiousPoint()
        void breakLoop()
        void abort()

    void for_each(...)
    void do_all(...)

    CPPAuto iterate[T](const T &, const T &)
    CPPAuto iterate[T](T &)

    cppclass SharedMemSys:
        SharedMemSys()

    cppclass loopname:
        loopname(char *name)

    cppclass no_pushes:
        no_pushes()

    cppclass steal:
        steal()

    cppclass disable_conflict_detection:
        disable_conflict_detection()


cdef extern from "katana/MethodFlags.h" namespace "katana" nogil:
    cppclass MethodFlag:
        bint operator==(MethodFlag)

    MethodFlag FLAG_UNPROTECTED "katana::MethodFlag::UNPROTECTED"
    MethodFlag FLAG_WRITE "katana::MethodFlag::WRITE"
    MethodFlag FLAG_READ "katana::MethodFlag::READ"
    MethodFlag FLAG_INTERNAL_MASK "katana::MethodFlag::INTERNAL_MASK"
    MethodFlag PREVIOUS "katana::MethodFlag::PREVIOUS"


cdef extern from "katana/Range.h" namespace "katana" nogil:
    cppclass StandardRange[it]:
        it begin()
        it end()


cdef extern from "katana/NoDerefIterator.h" namespace "katana" nogil:
    cppclass NoDerefIterator[it]:
        bint operator==(NoDerefIterator[it])
        bint operator!=(NoDerefIterator[it])
        NoDerefIterator[it] operator++()
        NoDerefIterator[it] operator--()
        it operator*()


cdef extern from "katana/Version.h" namespace "katana" nogil:
    string getVersion()
