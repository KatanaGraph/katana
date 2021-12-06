from libc.stdint cimport uint16_t

from katana.local.entity_type cimport EntityType


cdef class AtomicEntityType(EntityType):
    """
    A class representing node/edge actomice types.
    """
    @property
    def name(self):
        return self._name

    def __str__(self):
        """:rtype str: Name of the corresponding atomic type"""
        return self._name.decode("utf-8")

    @staticmethod
    cdef AtomicEntityType make(const EntityTypeManager *manager, uint16_t type_id):
        t = AtomicEntityType()
        t._type_manager = manager
        t._type_id = type_id
        r = manager.GetAtomicTypeName(type_id)
        if not r:
            raise LookupError("type id does not represent an atomic type")
        t._name = r.value()
        return t
