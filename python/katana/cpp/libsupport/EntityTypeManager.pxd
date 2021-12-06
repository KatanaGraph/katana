from libc.stdint cimport uint16_t
from libcpp cimport bool
from libcpp.set cimport set
from libcpp.string cimport string
from libcpp.unordered_map cimport unordered_map
from libcpp.vector cimport vector

from katana.cpp.libstd.optional cimport optional
from katana.cpp.libsupport.result cimport Result


cdef extern from "katana/EntityTypeManager.h" namespace "katana" nogil:

    ctypedef uint16_t EntityTypeID

    cdef cppclass EntityTypeManager:
        size_t GetNumAtomicTypes() const
        size_t GetNumEntityTypes() const
        bool HasAtomicType(const string& name) const
        vector[string] ListAtomicTypes() const
        bool HasEntityType(EntityTypeID entity_type_id) const
        EntityTypeID GetEntityTypeID(const string& name) const
        Result[set[string]] EntityTypeToTypeNameSet(EntityTypeID entity_type_id) const
        optional[string] GetAtomicTypeName(EntityTypeID entity_type_id) const
        vector[EntityTypeID] GetAtomicEntityTypeIDs() const
        bool IsSubtypeOf(EntityTypeID sub_type, EntityTypeID super_type) const
        const unordered_map[EntityTypeID, string]& GetEntityTypeIDToAtomicTypeNameMap() const
        size_t SetOfEntityTypeIDsSize() const
