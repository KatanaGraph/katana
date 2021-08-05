import operator

from numba import types
from numba.extending import overload

from katana.local.dynamic_bitset import DynamicBitset_numba_type


@overload(len)
def overload_DynamicBitset_len(self):
    if isinstance(self, DynamicBitset_numba_type):

        def impl(self):
            return self.size()

        return impl
    return None


@overload(operator.getitem)
def overload_DynamicBitset_getitem(self, i):
    if isinstance(self, DynamicBitset_numba_type) and isinstance(i, types.Integer):

        def impl(self, i):
            return self.get_item(i)

        return impl
    return None


@overload(operator.setitem)
def overload_DynamicBitset_setitem(self, i, v):
    if (
        isinstance(self, DynamicBitset_numba_type)
        and isinstance(i, types.Integer)
        and isinstance(v, (types.Boolean, types.Integer))
    ):

        def impl(self, i, v):
            self.set_item(i, v)

        return impl
    return None
