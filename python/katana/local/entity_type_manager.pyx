from libc.stdint cimport uint16_t
from libcpp.set cimport set
from libcpp.string cimport string
from libcpp.unordered_map cimport unordered_map
from libcpp.utility cimport move

from katana.cpp.libsupport.result cimport Result, raise_error_code


cdef set[string] handle_result_TypeNameSet(Result[set[string]] res) nogil except *:
    if not res.has_value():
        with gil:
            raise_error_code(res.error())
    return move(res.value())

cdef class EntityType:
    """
    A class representing node/edge types.
    """

    @property
    def type_id(self):
        return self._type_id

    def __str__(self):
        """:rtype str: Name of the corresponding atomic type"""
        typename_option = self._type_manager.GetAtomicTypeName(self._type_id)
        return typename_option.value().decode("utf-8")

cdef class AtomicEntityType(EntityType):
    """
    A class representing node/edge actomice types.
    """
    @property
    def name(self):
        return self._name.decode("utf-8")

    def __str__(self):
        """:rtype str: Name of the corresponding atomic type"""
        return self._name.decode("utf-8")

cdef class EntityTypeManager:

    @staticmethod
    cdef EntityTypeManager make(const CEntityTypeManager *manager):
        m = <EntityTypeManager>EntityTypeManager.__new__(EntityTypeManager)
        m.underlying_entity_type_manager = manager
        return m

    def _make_entity_type(self, type_id):
        t = EntityType()
        t._type_manager = self.underlying_entity_type_manager
        t._type_id = type_id
        return t

    def _make_atomic_entity_type(self, name):
        t = AtomicEntityType()
        t._type_manager = self.underlying_entity_type_manager
        t._name = name
        if not self.underlying_entity_type_manager.HasAtomicType(name):
            raise LookupError(f"{name} does not represent an atomic type")
        t._type_id = self.underlying_entity_type_manager.GetEntityTypeID(name)
        return t

    @property
    def atomic_types(self):
        """
        :return: a mapping from atomic type names to enity types.
        :rtype: dict[str, AtomicEntityType]
        """
        atomic_type_names = self.underlying_entity_type_manager.ListAtomicTypes()
        return {name: self._make_atomic_entity_type(name) for name in atomic_type_names}

    def is_subtype_of(self, sub_type, super_type):
        """
        :return: True iff the type ``sub_type`` is a subtype of the type ``super_type``
        """
        if isinstance(sub_type, int):
            sub_type_id = sub_type
        elif isinstance(sub_type, EntityType):
            sub_type_id = sub_type.type_id
        else:
            raise ValueError(f"{sub_type}'s type is not supported")
        if isinstance(super_type, int):
            super_type_id = super_type
        elif isinstance(super_type, EntityType):
            super_type_id = super_type.type_id
        else:
            raise ValueError(f"{super_type}'s type is not supported")

        if not self.underlying_entity_type_manager.HasEntityType(sub_type_id):
            raise LookupError(f"{sub_type} does not represent a type")
        if not self.underlying_entity_type_manager.HasEntityType(super_type_id):
            raise LookupError(f"{super_type} does not represent a type")
        return self.underlying_entity_type_manager.IsSubtypeOf(sub_type_id, super_type_id)
