from libc.stdint cimport uint16_t
from libcpp.string cimport string

from katana.cpp.libsupport.EntityTypeManager cimport EntityTypeManager


cdef class EntityType:
    cdef const EntityTypeManager *_type_manager
    cdef uint16_t _type_id
    @staticmethod
    cdef EntityType make(const EntityTypeManager *manager, uint16_t type_id)
