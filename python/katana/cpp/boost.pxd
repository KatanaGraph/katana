cdef extern from "boost/iterator/counting_iterator.hpp" namespace "boost" nogil:
    cppclass counting_iterator[I]:
        bint operator ==(counting_iterator[I])
        bint operator !=(counting_iterator[I])
        counting_iterator[I] operator++()
        counting_iterator[I] operator--()
        I operator *()
