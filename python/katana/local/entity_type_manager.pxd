from katana.cpp.libsupport.EntityTypeManager cimport EntityTypeManager as CEntityTypeManager


cdef class EntityTypeManager:

    #TODO(bowu): I don't know how to expose functions that use SetOfEntityTypeIDs

    cdef:
        const CEntityTypeManager *underlying_entity_type_manager

    @staticmethod
    cdef EntityTypeManager make(const CEntityTypeManager *manager)

