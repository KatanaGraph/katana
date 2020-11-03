import re
from abc import ABCMeta
from collections import namedtuple

NON_IDENTIFIER_CHAR_RE = re.compile(r"[^a-zA-Z0-9]")


def identifier_for_string(s):
    return NON_IDENTIFIER_CHAR_RE.sub("_", s)


class TypeInstantiation(metaclass=ABCMeta):
    element_c_type: str
    element_py_type: str
    by_pointer: bool
    fixed_dtype: str or None
    type_key: str

    def dtype(self, dynamic):
        return "np.dtype({})".format(self.fixed_dtype or dynamic)

    @property
    def type_scab(self):
        return identifier_for_string(self.element_c_type)


class PrimitiveTypeInstantiation(
    namedtuple("PrimitiveTypeInstantiation", ["element_c_type", "element_py_type",]), TypeInstantiation,
):
    @property
    def fixed_dtype(self):
        return self.element_py_type

    @property
    def by_pointer(self):
        return False

    @property
    def type_key(self):
        return self.element_py_type


primitive_type_instantiations = [
    PrimitiveTypeInstantiation("uint64_t", "np.uint64"),
    PrimitiveTypeInstantiation("int64_t", "int"),
    PrimitiveTypeInstantiation("uint32_t", "np.uint32"),
    PrimitiveTypeInstantiation("int32_t", "np.int32"),
    PrimitiveTypeInstantiation("double", "float"),
    PrimitiveTypeInstantiation("float", "np.float32"),
]


class OpaqueTypeInstantiation(namedtuple("OpaqueTypeInstantiation", ["size"]), TypeInstantiation):
    @property
    def fixed_dtype(self):
        return None

    @property
    def element_c_type(self):
        return "Opaque{}".format(self.size)

    @property
    def element_py_type(self):
        return "StructInstance"

    @property
    def by_pointer(self):
        return True

    @property
    def type_key(self):
        return self.size


opaque_type_instantiations = [OpaqueTypeInstantiation(s) for s in [8, 16, 32, 48, 64, 128]]

type_instantiations = primitive_type_instantiations + opaque_type_instantiations

type_instantiation_imports = """
import numpy as np
from libc.stdint cimport int64_t, uint64_t, int32_t, uint32_t
"""


exports = dict(
    primitive_type_instantiations=primitive_type_instantiations,
    opaque_type_instantiations=opaque_type_instantiations,
    type_instantiations=type_instantiations,
    type_instantiation_imports=type_instantiation_imports,
)
