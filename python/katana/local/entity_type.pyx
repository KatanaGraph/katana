from libc.stdint cimport uint8_t
from libcpp.string cimport string

from katana.cpp.libsupport.entity_type_manager cimport EntityTypeManager


cdef class EntityType:
    """
    A class representing node/edge types.

    Only works for atomic types. Intsersection type support TBD
    """

    @property
    def type_id(self):
        return self._type_id

    cdef string type_name(self):
        typename_option = self._type_manager.GetAtomicTypeName(self._type_id)
        return typename_option.value()

    def __str__(self):
        """:rtype string: Name of the corresponding atomic type"""
        return self.type_name().decode("utf-8")

    @staticmethod
    cdef EntityType Make(const EntityTypeManager *manager, uint8_t type_id):
        t = EntityType()
        t._type_manager = manager
        t._type_id = type_id
        return t
