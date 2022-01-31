#include "katana/EntityTypeManager.h"

#include <pybind11/pybind11.h>
#include <pybind11/stl.h>

#include "katana/python/Conventions.h"
#include "katana/python/CythonIntegration.h"
#include "katana/python/ErrorHandling.h"
#include "katana/python/PythonModuleInitializers.h"

namespace py = pybind11;

namespace {

struct EntityType {
  const katana::EntityTypeManager* owner;
  const katana::EntityTypeID type_id;

  bool operator==(const EntityType& other) const {
    return owner == other.owner && type_id == other.type_id;
  }

  std::string ToString() {
    auto r = owner->GetAtomicTypeName(type_id);
    if (r) {
      return r.value();
    } else {
      return fmt::format("<non-atomic type {}>", type_id);
    }
  }
};

struct AtomicEntityType : public EntityType {
  std::string name() { return ToString(); }
};

katana::SetOfEntityTypeIDs
GetSetOfEntityTypeIds(std::vector<EntityType> types) {
  katana::SetOfEntityTypeIDs type_ids;
  type_ids.resize(types.size());
  for (auto t : types) {
    type_ids.set(t.type_id);
  }
  return type_ids;
}

}  // namespace

void
katana::python::InitEntityTypeManager(py::module_& m) {
  katana::DefConventions(py::class_<EntityType>(m, "EntityType"))
      // Expose a field, read only
      .def_readonly("id", &EntityType::type_id)
      .def("__hash__", [](EntityType* self) { return self->type_id; });

  py::class_<AtomicEntityType, EntityType>(m, "AtomicEntityType")
      .def_property_readonly("name", &AtomicEntityType::ToString);

  py::class_<katana::EntityTypeManager> entity_type_manager_cls(
      m, "EntityTypeManager");

  katana::DefConventions(entity_type_manager_cls);
  katana::DefCythonSupport(entity_type_manager_cls);
  entity_type_manager_cls.def(py::init<>())
      .def_property_readonly(
          "atomic_types",
          [](const katana::EntityTypeManager* self) {
            py::dict ret;
            for (auto& id : self->GetAtomicEntityTypeIDs()) {
              AtomicEntityType type{{self, id}};
              ret[py::cast(type.name())] = type;
            }
            return ret;
            // pybind11 automatically extends the lifetime of self until ret is freed.
          })
      // Overloads work similarly to C++.
      .def(
          "is_subtype_of",
          [](const katana::EntityTypeManager* self, EntityType sub_type,
             EntityType super_type) {
            if (sub_type.owner != self || super_type.owner != self) {
              throw py::value_error("EntityTypes must be owned by self.");
            }
            return self->IsSubtypeOf(sub_type.type_id, super_type.type_id);
          },
          py::arg("sub_type"), py::arg("super_type"))
      .def(
          "is_subtype_of",
          [](const katana::EntityTypeManager* self,
             katana::EntityTypeID sub_type, katana::EntityTypeID super_type) {
            return self->IsSubtypeOf(sub_type, super_type);
          },
          py::arg("sub_type"), py::arg("super_type"))
      .def(
          "add_atomic_entity_type",
          &katana::EntityTypeManager::AddAtomicEntityType)
      .def(
          "get_non_atomic_entity_type",
          [](katana::EntityTypeManager* self,
             std::vector<EntityType> types) -> Result<EntityType> {
            auto set_of_type_ids = GetSetOfEntityTypeIds(types);
            return EntityType{
                self,
                KATANA_CHECKED(self->GetNonAtomicEntityType(set_of_type_ids))};
          })
      .def(
          "get_or_add_non_atomic_entity_type",
          [](katana::EntityTypeManager* self,
             std::vector<EntityType> types) -> Result<EntityType> {
            auto set_of_type_ids = GetSetOfEntityTypeIds(types);
            return EntityType{
                self, KATANA_CHECKED(
                          self->GetOrAddNonAtomicEntityType(set_of_type_ids))};
          })
      .def(
          "type_from_id",
          [](const katana::EntityTypeManager* self, katana::EntityTypeID id) {
            if (self->GetAtomicTypeName(id).has_value()) {
              return py::cast(AtomicEntityType{{self, id}});
            } else {
              return py::cast(EntityType{self, id});
            }
          })
      .def(
          "get_atomic_subtypes",
          [](const katana::EntityTypeManager* self, EntityType type) {
            py::set ret;
            if (self->HasEntityType(type.type_id)) {
              auto type_set = self->GetAtomicSubtypes(type.type_id);
              for (EntityTypeID subtype_id = 0; subtype_id < type_set.size();
                   ++subtype_id) {
                if (type_set.test(subtype_id)) {
                  if (self->GetAtomicTypeName(subtype_id).has_value()) {
                    ret.add(AtomicEntityType{{self, subtype_id}});
                  }
                }
              }
            }
            return ret;
          })
      .def(
          "get_supertypes",
          [](const katana::EntityTypeManager* self, EntityType type) {
            py::set ret;
            if (self->GetAtomicTypeName(type.type_id).has_value()) {
              auto type_set = self->GetSupertypes(type.type_id);
              for (EntityTypeID subtype_id = 0; subtype_id < type_set.size();
                   ++subtype_id) {
                if (type_set.test(subtype_id)) {
                  ret.add(EntityType{self, subtype_id});
                }
              }
            }
            return ret;
          })
      .def(
          "get_num_atomic_types", &katana::EntityTypeManager::GetNumAtomicTypes)
      .def(
          "get_num_of_entity_types",
          &katana::EntityTypeManager::GetNumEntityTypes);
}
