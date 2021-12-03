from katana.cpp.libsupport.EntityTypeManager cimport EntityTypeManager as CEntityTypeManager

cdef class EntityTypeManager:
    cdef:
        const CEntityTypeManager *underlying_entity_type_manager

    @staticmethod
    cdef EntityTypeManager make(const CEntityTypeManager *manager)

