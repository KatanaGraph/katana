cdef class EntityTypeManager:

    @staticmethod
    cdef EntityTypeManager make(const CEntityTypeManager *manager):
        m = <EntityTypeManager>EntityTypeManager.__new__(EntityTypeManager)
        m.underlying_entity_type_manager = manager
        return m

    def get_atomic_entity_type_ids(self):
        """
        Return a list of all entity type IDs
        """
        return self.underlying_entity_type_manager.GetAtomicEntityTypeIDs()
