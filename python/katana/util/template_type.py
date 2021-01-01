from functools import wraps, partial, lru_cache

import numpy as np

from katana.util import wraps_class


def find_size_for_dtype(dtype, sizes=(8, 16, 32, 48, 64, 128)):
    size = dtype.itemsize
    for s in sizes:
        if s >= size:
            return s
    raise TypeError("Dtypes up to " + str(s) + " bytes are supported. Dtype is " + str(size) + " bytes.")


class DtypeDict(dict):
    """
    A dict which converts keys to dtypes before using.
    """

    def __init__(self):
        # Only allow empty constructor for simplicity.
        super(DtypeDict, self).__init__()

    def __setitem__(self, key, value):
        super(DtypeDict, self).__setitem__(np.dtype(key), value)

    def __getitem__(self, key):
        return super(DtypeDict, self).__getitem__(np.dtype(key))


class DtypeDictWithOpaque(dict):
    def __init__(self):
        super(DtypeDictWithOpaque, self).__init__()
        self._sizes_cache = None

    @property
    def _sizes(self):
        if self._sizes_cache is None:
            sizes = list(filter(lambda k: isinstance(k, int), self.keys()))
            sizes.sort()
            self._sizes_cache = sizes
        return self._sizes_cache

    def __setitem__(self, key, value):
        if isinstance(key, int):
            super(DtypeDictWithOpaque, self).__setitem__(key, value)
        else:
            key = np.dtype(key)
            if key.kind != "V":
                super(DtypeDictWithOpaque, self).__setitem__(key, value)
            else:
                raise ValueError("Do not explicitly add struct types to DtypeDictWithOpaque")

    def __getitem__(self, key):
        key = np.dtype(key)
        if key in self:
            return super(DtypeDictWithOpaque, self).__getitem__(key)(key)
        if key.kind != "V":
            raise KeyError(key)
        # Handle struct types
        v = super(DtypeDictWithOpaque, self).__getitem__(find_size_for_dtype(key, self._sizes))
        return v(dtype=key)


class TemplateType1(type):
    def __new__(cls, name, bases, attrs):
        instantiations = {(t if isinstance(t, int) else np.dtype(t)): v for t, v in attrs["instantiations"].items()}
        del attrs["instantiations"]
        representative_type = next(iter(instantiations.values()))
        attrs.update(representative_type.__dict__)
        attrs["_instantiations"] = instantiations
        attrs["_representative"] = representative_type

        @wraps(representative_type.__init__)
        def _init_stub(self, *_args, **_kwargs):
            name = type(self).__name__
            raise TypeError("{0} cannot be instantiated directly. Select a specific type with {0}[...].".format(name))

        attrs["__init__"] = _init_stub
        return type.__new__(cls, name, (), attrs)

    def __getitem__(cls, item):
        return cls._instantiations[np.dtype(item)]

    def __instancecheck__(cls, instance):
        return any(isinstance(instance, ty) for ty in cls._instantiations.values())

    def __subclasscheck__(cls, subclass):
        return any(issubclass(subclass, ty) for ty in cls._instantiations.values())

    def __repr__(cls):
        return "<template class '{}.{}'>".format(cls._representative.__module__, cls.__qualname__)


class TemplateType1WithOpaque(TemplateType1):
    def __init__(cls, name, bases, attrs):
        super(TemplateType1WithOpaque, cls).__init__(name, bases, attrs)
        sizes = list(filter(lambda k: isinstance(k, int), cls._instantiations.keys()))
        sizes.sort()
        cls._sizes = sizes

    @lru_cache(maxsize=None)
    def __getitem__(cls, item):
        item = np.dtype(item)
        try:
            inst_cls = cls._instantiations[item]
        except KeyError:
            if item.kind != "V":
                raise
            # Handle struct types
            size = find_size_for_dtype(item, cls._sizes)
            inst_cls = cls._instantiations[size]

        @wraps_class(inst_cls, str(item))
        class SubClass(inst_cls):
            def __init__(self, *args, **kwargs):
                super(SubClass, self).__init__(*args, dtype=item, **kwargs)

        return SubClass


def make_template_type1(name, instantiations):
    return TemplateType1(name, None, dict(instantiations=instantiations))


def make_template_type1_with_opaque(name, instantiations):
    return TemplateType1WithOpaque(name, None, dict(instantiations=instantiations))
