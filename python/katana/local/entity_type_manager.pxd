from libc.stdint cimport uint16_t
from libcpp.string cimport string

from katana.cpp.libsupport.EntityTypeManager cimport EntityTypeManager as CEntityTypeManager


cdef class EntityType:
    cdef const CEntityTypeManager *_type_manager
    cdef uint16_t _type_id

cdef class AtomicEntityType(EntityType):
    cdef string _name

cdef class EntityTypeManager:

    #TODO(bowu): I don't know how to expose functions that use SetOfEntityTypeIDs

    cdef:
        const CEntityTypeManager *underlying_entity_type_manager

    @staticmethod
    cdef EntityTypeManager make(const CEntityTypeManager *manager)

