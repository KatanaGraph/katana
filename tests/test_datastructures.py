import pytest
import numpy as np

from galois.datastructures import InsertBag, LargeArray, AllocationPolicy
from galois.loops import do_all_operator, do_all

types = [
    pytest.param(int, id="int"),
    pytest.param(float, id="float"),
    pytest.param(np.uint64, id="uint64_t"),
    pytest.param(np.int32, id="int32_t"),
]


@pytest.mark.parametrize("typ", types)
def test_InsertBag_simple(typ):
    T = InsertBag[typ]
    assert issubclass(T, InsertBag)
    bag = T()
    assert isinstance(bag, InsertBag)
    assert isinstance(bag, InsertBag[typ])
    assert bag.empty()
    bag.push(10)
    bag.push(2)
    bag.push(3)
    assert not bag.empty()
    assert set(bag) == {10, 2, 3}
    bag.clear()
    assert bag.empty()
    assert set(bag) == set()


def test_InsertBag_opaque():
    dt = np.dtype([("x", np.float32), ("y", np.int8),], align=True)
    T = InsertBag[dt]
    assert issubclass(T, InsertBag)
    bag = T()
    assert isinstance(bag, InsertBag)
    assert isinstance(bag, InsertBag[dt])
    assert bag.empty()
    bag.push((10.1, 11))
    assert not bag.empty()
    for s in bag:
        assert s.x == pytest.approx(10.1)
        assert s.y == 11
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


def test_InsertBag_parallel_opaque():
    dt = np.dtype([("x", np.float32), ("y", np.int16),], align=True)
    T = InsertBag[dt]
    bag = T()

    @do_all_operator()
    def f(bag, i):
        bag.push((i / 2.0, i))

    do_all(range(1000), f(bag), steal=False)
    for s in bag:
        assert s.x == pytest.approx(s.y / 2.0)


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


def test_LargeArray_opaque():
    dt = np.dtype([("x", np.float32), ("y", np.int16),], align=True)
    T = LargeArray[dt]
    assert issubclass(T, LargeArray)
    arr = T()
    assert isinstance(arr, LargeArray)
    assert isinstance(arr, T)
    arr.allocateInterleaved(5)
    arr[0] = (1.1, 2)
    arr[3] = (1.1, 2)
    with pytest.raises(IndexError):
        arr[10] = 0
    assert arr[0].x == pytest.approx(1.1)
    assert arr[0].y == 2
    assert arr[3].x == pytest.approx(1.1)
    assert arr[3].y == 2
    nparr = arr.as_numpy()
    assert nparr[0]["x"] == pytest.approx(1.1)
    assert nparr[0]["y"] == 2
    assert nparr[3]["x"] == pytest.approx(1.1)
    assert nparr[3]["y"] == 2


def test_LargeArray_opaque_extra_space():
    dt = np.dtype([("x", np.int8), ("y", np.int8),], align=True)
    arr = LargeArray[dt]()
    arr.allocateInterleaved(5)
    arr[0] = (1, 2)
    arr[2] = (2, 5)
    assert arr[0].x == 1
    assert arr[0].y == 2
    assert arr[2].x == 2
    assert arr[2].y == 5
    nparr = arr.as_numpy()
    assert nparr[0]["x"] == 1
    assert nparr[0]["y"] == 2
    assert nparr[2]["x"] == 2
    assert nparr[2]["y"] == 5


@pytest.mark.parametrize("typ", types)
def test_LargeArray_constructor(typ):
    T = LargeArray[typ]
    arr = T(8, AllocationPolicy.BLOCKED)
    assert len(arr) == 8


@pytest.mark.parametrize("typ", types)
def test_LargeArray_realloc(typ):
    T = LargeArray[typ]
    arr = T()
    arr.allocateInterleaved(10)
    with pytest.raises(ValueError):
        arr.allocateInterleaved(10)


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


@pytest.mark.parametrize("typ", types)
def test_LargeArray_numpy(typ):
    T = LargeArray[typ]
    larr = T()
    with pytest.raises(ValueError):
        larr.as_numpy()
    larr.allocateBlocked(5)
    arr: np.ndarray = larr.as_numpy()
    arr[0] = 10
    arr[4] = 10
    arr[1:4] = 1
    assert list(arr) == [10, 1, 1, 1, 10]
    assert list(larr) == [10, 1, 1, 1, 10]
    # TODO: Ideally one level of .base would get us there, but it doesn't really matter
    assert arr.base.base.base is larr
    assert arr.shape == (5,)
    with pytest.raises(IndexError):
        arr[10] = 0


@pytest.mark.parametrize("typ", types)
def test_LargeArray_numpy_parallel(typ):
    T = LargeArray[typ]
    arr = T()
    arr.allocateInterleaved(1000)

    @do_all_operator()
    def f(arr, i):
        arr[i] = i
        arr[i] += 1

    do_all(range(1000), f(arr.as_numpy()), steal=False)
    assert list(arr) == list(range(1, 1001))


def test_LargeArray_numpy_parallel_opaque():
    dt = np.dtype([("x", np.float32), ("y", np.int16),], align=True)
    T = LargeArray[dt]
    arr = T()
    arr.allocateInterleaved(1000)

    @do_all_operator()
    def f(arr, i):
        arr[i].x = i
        arr[i].y = i
        arr[i].x += 1.1

    do_all(range(1000), f(arr.as_numpy()), steal=False)

    for i, s in enumerate(arr):
        assert s.x == pytest.approx(i + 1.1)
        assert s.y == i
        assert arr[i].x == pytest.approx(i + 1.1)
        assert arr[i].y == i


def test_InsertBag_numba_type():
    import numba.types
    from galois.datastructures import InsertBag_numba_type

    assert isinstance(InsertBag_numba_type[int], numba.types.Type)

    dt = np.dtype([("x", np.float32), ("y", np.int8),], align=True)
    assert isinstance(InsertBag_numba_type[dt], numba.types.Type)


def test_LargeArray_numba_type():
    import numba.types
    from galois.datastructures import LargeArray_numba_type

    assert isinstance(LargeArray_numba_type[int], numba.types.Type)

    dt = np.dtype([("x", np.float32), ("y", np.int8),], align=True)
    assert isinstance(LargeArray_numba_type[dt], numba.types.Type)
