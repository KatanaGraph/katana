from libc.stdint cimport uint16_t
from libcpp.string cimport string

from katana.cpp.libsupport.EntityTypeManager cimport EntityTypeManager


cdef class EntityType:
    """
    A class representing node/edge types.

    Only works for atomic types. Intsersection type support TBD
    """

    @property
    def type_id(self):
        return self._type_id

    def __str__(self):
        """:rtype str: Name of the corresponding atomic type"""
        typename_option = self._type_manager.GetAtomicTypeName(self._type_id)
        return typename_option.value().decode("utf-8")

    @staticmethod
    cdef EntityType make(const EntityTypeManager *manager, uint16_t type_id):
        t = EntityType()
        t._type_manager = manager
        t._type_id = type_id
        return t
