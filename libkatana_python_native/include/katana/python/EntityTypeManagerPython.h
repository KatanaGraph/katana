#ifndef KATANA_LIBKATANAPYTHONNATIVE_KATANA_PYTHON_ENTITYTYPEMANAGERPYTHON_H_
#define KATANA_LIBKATANAPYTHONNATIVE_KATANA_PYTHON_ENTITYTYPEMANAGERPYTHON_H_

#include <string>

#include "katana/EntityTypeManager.h"

namespace katana::python {

class EntityType {
public:
  const EntityTypeManager* owner;
  const EntityTypeID type_id;

  EntityType(const EntityTypeManager* _owner, const EntityTypeID _type_id)
      : owner(_owner), type_id(_type_id) {}

  bool operator==(const EntityType& other) const {
    return owner == other.owner && type_id == other.type_id;
  }

  std::string ToString() const;

  static std::unique_ptr<EntityType> Make(
      const katana::EntityTypeManager* self, katana::EntityTypeID id);
};

class AtomicEntityType : public EntityType {
public:
  using EntityType::EntityType;

  std::string name() { return ToString(); }
};

}  // namespace katana::python

#endif
