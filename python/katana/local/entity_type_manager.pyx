from libcpp.set cimport set
from libcpp.string cimport string
from libcpp.unordered_map cimport unordered_map
from libcpp.utility cimport move

from katana.cpp.libsupport.result cimport Result, raise_error_code
from katana.local.atomic_entity_type cimport AtomicEntityType
from katana.local.entity_type cimport EntityType


cdef set[string] handle_result_TypeNameSet(Result[set[string]] res) nogil except *:
    if not res.has_value():
        with gil:
            raise_error_code(res.error())
    return move(res.value())

cdef class EntityTypeManager:

    @staticmethod
    cdef EntityTypeManager make(const CEntityTypeManager *manager):
        m = <EntityTypeManager>EntityTypeManager.__new__(EntityTypeManager)
        m.underlying_entity_type_manager = manager
        return m

    def get_num_atomic_types(self):
        """
        Return the number of atomic types
        """
        return self.underlying_entity_type_manager.GetNumAtomicTypes()

    def get_num_entity_types(self):
        """
        Return the number of entity types (including kUnknownEntityType)
        """
        return self.underlying_entity_type_manager.GetNumEntityTypes()

    def has_atomic_type(self, atomic_type):
        """
        Return true iff an atomic type ``name`` exists
        """
        if isinstance(atomic_type, str):
            name_string = atomic_type
        elif isinstance(atomic_type, AtomicEntityType):
            name_string = atomic_type.name
        else:
            raise ValueError(f"{atomic_type}'s type is not supported")
        cdef string input_str = bytes(name_string, "utf-8")
        return self.underlying_entity_type_manager.HasAtomicType(input_str)

    def list_atomic_types(self):
        """
        Return a list of integers representing all the available atomic types
        """
        return self.underlying_entity_type_manager.ListAtomicTypes()

    def has_entity_type(self, entity_type):
        """
        Return true iff an entity type ``entity_type`` exists
        """
        if isinstance(entity_type, int):
            entity_type_id = entity_type
        elif isinstance(entity_type, EntityType):
            entity_type_id = entity_type.type_id
        else:
            raise ValueError(f"{entity_type}'s type is not supported")
        return self.underlying_entity_type_manager.HasEntityType(entity_type_id)

    def get_entity_type_id(self, atomic_type):
        """
        Return the EntityTypeID for an atomic type with ``atomic_type``
        """
        if isinstance(atomic_type, str):
            name_string = atomic_type
        elif isinstance(atomic_type, AtomicEntityType):
            name_string = atomic_type.name
        else:
            raise ValueError(f"{atomic_type}'s type is not supported")
        cdef string input_str = bytes(name_string, "utf-8")
        if not self.underlying_entity_type_manager.HasAtomicType(input_str):
          raise LookupError(f"{atomic_type} does not represent an atomic type")
        return self.underlying_entity_type_manager.GetEntityTypeID(input_str)

    def entity_type_to_type_name_set(self, entity_type):
        """
        Return a set of strings representing the names of the types for the entity type
        ``entity_type``
        """
        if isinstance(entity_type, int):
            entity_type_id = entity_type
        elif isinstance(entity_type, EntityType):
            entity_type_id = entity_type.type_id
        else:
            raise ValueError(f"{entity_type}'s type is not supported")
        if not self.underlying_entity_type_manager.HasEntityType(entity_type_id):
            raise LookupError(f"{entity_type_id} does not represent a type")
        return handle_result_TypeNameSet(self.underlying_entity_type_manager.EntityTypeToTypeNameSet(entity_type_id))

    def get_atomic_type_name(self, entity_type):
        """
        Return the type name if ``entity_type`` is a valid entity type or entity type ID. Otherwise,
        return None
        """
        if isinstance(entity_type, int):
            entity_type_id = entity_type
        elif isinstance(entity_type, EntityType):
            entity_type_id = entity_type.type_id
        else:
            raise ValueError(f"{entity_type}'s type is not supported")

        r =  self.underlying_entity_type_manager.GetAtomicTypeName(entity_type_id)
        if not r:
            raise LookupError(f"{entity_type_id} does not represent an atomic type")
        return r.value()

    def get_atomic_entity_type_ids(self):
        """
        Return a list of all entity type IDs
        """
        return self.underlying_entity_type_manager.GetAtomicEntityTypeIDs()

    def is_subtype_of(self, sub_type, super_type):
        """
        Return True iff the type ``sub_type`` is a subtype of the type ``super_type``
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

    def get_entity_type_id_to_atomic_type_name_map(self):
        """
        Return the dict from entity type IDs to type names
        """
        return self.underlying_entity_type_manager.GetEntityTypeIDToAtomicTypeNameMap()

    def all_atomic_types(self):
        """
        Return a list of all atomic types
        """
        manager = self.underlying_entity_type_manager
        return [AtomicEntityType.make(manager, type_id) for type_id in self.get_atomic_entity_type_ids()]
