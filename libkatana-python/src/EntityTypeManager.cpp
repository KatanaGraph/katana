#include "katana/EntityTypeManager.h"

#include <pybind11/pybind11.h>

#include "AllModules.h"
#include "Conventions.h"
#include "ErrorHandling.h"

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

}  // namespace

void
InitEntityTypeManager(py::module_& m) {
  katana::DefConventions(py::class_<EntityType>(m, "EntityType"))
      // Expose a field, read only
      .def_readonly("id", &EntityType::type_id);

  py::class_<AtomicEntityType, EntityType>(m, "AtomicEntityType")
      .def_property_readonly("name", &AtomicEntityType::ToString);

  katana::DefConventions(
      py::class_<katana::EntityTypeManager>(m, "EntityTypeManager"))
      .def(py::init<>())
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
          &katana::EntityTypeManager::AddAtomicEntityType);
}
