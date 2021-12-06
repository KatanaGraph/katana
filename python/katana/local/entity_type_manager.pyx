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

    def has_atomic_type(self, str name):
        """
        Return true iff an atomic type `name` exists
        """
        cdef string input_str = bytes(name, "utf-8")
        return self.underlying_entity_type_manager.HasAtomicType(input_str)

    def list_atomic_types(self):
        """
        Return a list of integers representing all the available atomic types
        """
        return self.underlying_entity_type_manager.ListAtomicTypes()

    def has_entity_type(self, entity_type_id: int):
        """
        Return true iff an entity type `entity_type_id` exists
        """
        return self.underlying_entity_type_manager.HasEntityType(entity_type_id)

    def get_entity_type_id(self, str name):
        """
        Return the EntityTypeID for an atomic type with `name`
        """
        cdef string input_str = bytes(name, "utf-8")
        return self.underlying_entity_type_manager.GetEntityTypeID(input_str)

    def entity_type_to_type_name_set(self, entity_type_id: int):
        """
        Return a set of strings representing the names of the types for the type ID
        `entity_type_id`
        """
        return handle_result_TypeNameSet(self.underlying_entity_type_manager.EntityTypeToTypeNameSet(entity_type_id))

    def get_atomic_type_name(self, entity_type_id: int):
        """
        Return the type name if `entity_type_id` is a valid entity type ID. Otherwise,
        return None
        """

        r =  self.underlying_entity_type_manager.GetAtomicTypeName(entity_type_id)
        if r:
            return r.value()
        return None

    def get_atomic_entity_type_ids(self):
        """
        Return a list of all entity type IDs
        """
        return self.underlying_entity_type_manager.GetAtomicEntityTypeIDs()

    def is_subtype_of(self, sub_type: int, super_type: int):
        """
        Return True iff the type `sub_type` is a subtype of the type `super_type`
        """
        return self.underlying_entity_type_manager.IsSubtypeOf(sub_type, super_type)

    def get_entity_type_id_to_atomic_type_name_map(self):
        """
        Return the map from entity type IDs to type names
        """
        return self.underlying_entity_type_manager.GetEntityTypeIDToAtomicTypeNameMap()

    def set_of_entity_type_ids_size(self):
        """
        Return the current size of the SetOFEntityTypeIDs bitsets
        """
        return self.underlying_entity_type_manager.SetOfEntityTypeIDsSize()
