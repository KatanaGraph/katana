import pytest
import numpy as np

from galois.loops import *
from galois.datastructures import *

types = [
    pytest.param(int, id="int"),
    pytest.param(float, id="float"),
    pytest.param(np.uint64, id="uint64_t"),
]

@pytest.mark.parametrize("typ", types)
def test_InsertBag_simple(typ):
    T = InsertBag[typ]
    assert issubclass(T, InsertBag)
    bag = T()
    assert isinstance(bag, InsertBag)
    assert isinstance(bag, T)
    assert bag.empty()
    bag.push(10)
    bag.push(2)
    bag.push(3)
    assert not bag.empty()
    assert set(bag) == {10, 2, 3}
    bag.clear()
    assert bag.empty()
    assert set(bag) == set()

@pytest.mark.parametrize("typ", types)
def test_InsertBag_parallel(typ):
    T = InsertBag[typ]
    bag = T()
    @do_all_operator()
    def f(bag, i):
        bag.push(i)
        bag.push(i)
    do_all(range(1000), f(bag), steal=False)
    l = list(bag)
    l.sort()
    assert l == [v for i in range(1000) for v in [i, i]]

@pytest.mark.parametrize("typ", types)
def test_LargeArray_simple(typ):
    T = LargeArray[typ]
    assert issubclass(T, LargeArray)
    arr = T()
    assert isinstance(arr, LargeArray)
    assert isinstance(arr, T)
    arr.allocateInterleaved(5)
    arr[0] = 10
    arr[1] = 1
    arr[4] = 10
    with pytest.raises(IndexError):
        arr[10] = 0
    assert list(arr) == [10, 1, 0, 0, 10]

@pytest.mark.parametrize("typ", types)
def test_LargeArray_parallel(typ):
    T = LargeArray[typ]
    arr = T()
    arr.allocateInterleaved(1000)
    @do_all_operator()
    def f(arr, i):
        # TODO: Use __setitem__
        arr.set(i, i)
        arr.set(i, arr.get(i) + 1)
    do_all(range(1000), f(arr), steal=False)
    assert list(arr) == list(range(1, 1001))
