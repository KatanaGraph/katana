from libc.stdint cimport uint8_t
from libcpp.string cimport string

from katana.cpp.libsupport.entity_type_manager cimport EntityTypeManager


cdef class EntityType:
    cdef const EntityTypeManager *_type_manager
    cdef uint8_t _type_id
    cdef string type_name(self)
    @staticmethod
    cdef EntityType Make(const EntityTypeManager *manager, uint8_t type_id)
