import numpy as np

from katana.local_native import EntityTypeManager


class EntityTypeArray(EntityTypeManager):
    """
    An array of entity types and their associated relationships.
    """

    def __init__(self, size: int):
        super().__init__()
        self._data = np.full(size, self.unknown_type.id, dtype=np.uint16)

    @classmethod
    def from_type_names(cls, type_names) -> "EntityTypeArray":
        """
        Create an entity type array from a sequence of type names. No entity may have more than one type.

        :param type_names: An iterable of type names. This can be a pandas categorical or a list of strings or similar.
        """
        self = cls(len(type_names))
        for i, name in enumerate(type_names):
            self[i] = self.get_or_add_atomic_entity_type(name)
        return self

    @classmethod
    def from_type_name_sets(cls, type_name_sets) -> "EntityTypeArray":
        """
        Create an entity type array from a sequence of sets of type names.

        :param type_names: An iterable of sets of type names. This can be a list of contains of a numpy or pandas array
            or objects.

        :note: Do not pass a sequence of strings to this method as it will treat the strings as sequences of single
            character type names.
        """
        self = cls(len(type_name_sets))
        for i, name_set in enumerate(type_name_sets):
            self[i] = self.get_or_add_non_atomic_entity_type(self.get_or_add_atomic_entity_type(n) for n in name_set)
        return self

    def __setitem__(self, i, typ):
        self._data[i] = typ.id

    def __getitem__(self, i):
        return self.type_from_id(self._data[i])

    def __len__(self):
        return len(self._data)

    def __iter__(self):
        return (self.type_from_id(v) for v in self._data)
