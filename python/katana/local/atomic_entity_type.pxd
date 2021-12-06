from libc.stdint cimport uint16_t
from libcpp.string cimport string

from katana.cpp.libsupport.EntityTypeManager cimport EntityTypeManager
from katana.local.entity_type cimport EntityType


cdef class AtomicEntityType(EntityType):
    cdef string _name
    @staticmethod
    cdef AtomicEntityType make(const EntityTypeManager *manager, uint16_t type_id)
