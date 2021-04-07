from libcpp.string cimport string 

cdef extern from "katana/Version.h" namespace "katana" nogil:
    cdef string getVersion()
    cdef string getRevision()
    cdef int getVersionMajor()
    cdef int getVersionMinor()
    cdef int getVersionPatch()
    cdef int getCopyrightYear()