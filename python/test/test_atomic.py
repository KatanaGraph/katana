# pylint: disable=unused-argument

import numpy as np
import pytest

from katana import do_all, do_all_operator
from katana.local import (
    NUMAArray,
    ReduceAnd,
    ReduceMax,
    ReduceMin,
    ReduceOr,
    ReduceSum,
    atomic_add,
    atomic_max,
    atomic_min,
    atomic_sub,
)

dtypes_int = [
    pytest.param(np.int64, id="int64"),
    pytest.param(np.uint64, id="uint64"),
]
dtypes = dtypes_int + [
    pytest.param(np.float, id="float"),
]

types = [
    pytest.param(int, id="int"),
    pytest.param(float, id="float"),
    pytest.param(np.uint64, id="uint64_t"),
]

acc_types = [
    pytest.param(ReduceSum, 15, id="ReduceSum"),
    pytest.param(ReduceMax, 10, id="ReduceMax"),
    pytest.param(ReduceMin, 2, id="ReduceMin"),
]


@pytest.mark.parametrize("acc_type,res", acc_types)
@pytest.mark.parametrize("typ", types)
def test_accumulator_simple(acc_type, res, typ):
    T = acc_type[typ]
    acc = T()
    acc.update(10)
    acc.update(2)
    acc.update(3)
    assert acc.reduce() == res
    acc = T(10)
    assert acc.reduce() == 10


def test_ReduceSum_parallel(threads_many):
    T = ReduceSum[int]
    acc = T()

    @do_all_operator()
    def f(acc, i):
        acc.update(i)

    do_all(range(1000), f(acc), steal=False)
    assert acc.reduce() == 499500


def test_ReduceMax_parallel(threads_many):
    T = ReduceMax[int]
    acc = T()

    @do_all_operator()
    def f(acc, i):
        acc.update(abs(500 - i))

    do_all(range(1000), f(acc), steal=False)
    assert acc.reduce() == 500


def test_ReduceMin_parallel(threads_many):
    T = ReduceMin[float]
    acc = T()

    @do_all_operator()
    def f(acc, i):
        acc.update((i - 500) / 10)

    do_all(range(1000), f(acc), steal=False)
    assert acc.reduce() == -50.0


def test_ReduceOr_parallel(threads_many):
    T = ReduceOr
    acc = T()

    @do_all_operator()
    def f(acc, i):
        acc.update(i % 3 == 0)

    do_all(range(1000), f(acc), steal=False)
    assert acc.reduce() is True


def test_ReduceAnd_parallel(threads_many):
    T = ReduceAnd
    acc = T()

    @do_all_operator()
    def f(acc, i):
        acc.update(i % 3 == 0)

    do_all(range(1000), f(acc), steal=False)
    assert acc.reduce() is False


def test_ReduceOr_simple():
    T = ReduceOr
    acc = T()
    acc.update(True)
    acc.update(False)
    acc.update(True)
    assert acc.reduce() is True
    acc = T(True)
    assert acc.reduce() is True
    acc = T(False)
    assert acc.reduce() is False


def test_ReduceAnd_simple():
    T = ReduceAnd
    acc = T()
    acc.update(True)
    acc.update(False)
    acc.update(True)
    assert acc.reduce() is False
    acc = T(True)
    assert acc.reduce() is True
    acc = T(False)
    assert acc.reduce() is False


@pytest.mark.parametrize("dtype", dtypes)
def test_atomic_add_parallel(dtype, threads_many):
    @do_all_operator()
    def f(out, i):
        atomic_add(out, 0, i)

    out = np.array([0], dtype=dtype)
    do_all(range(1000), f(out), steal=False)
    assert out[0] == 499500


def test_atomic_add_parallel_numaarray(threads_many):
    @do_all_operator()
    def f(out, i):
        atomic_add(out, 0, i)

    out = NUMAArray[int]()
    out.allocateBlocked(1000)
    do_all(range(1000), f(out.as_numpy()), steal=False)
    assert out[0] == 499500


@pytest.mark.parametrize("dtype", dtypes)
def test_atomic_sub_parallel(dtype, threads_many):
    @do_all_operator()
    def f(out, i):
        atomic_sub(out, 0, i)

    out = np.array([499500], dtype=dtype)
    do_all(range(1000), f(out), steal=False)
    assert out[0] == 0


@pytest.mark.parametrize("dtype", dtypes_int)
def test_atomic_max_parallel(dtype, threads_many):
    @do_all_operator()
    def f(out, i):
        atomic_max(out, 0, i)

    out = np.array([500], dtype=dtype)
    do_all(range(1000), f(out), steal=False)
    assert out[0] == 999


@pytest.mark.parametrize("dtype", dtypes_int)
def test_atomic_min_parallel(dtype, threads_many):
    @do_all_operator()
    def f(out, i):
        atomic_min(out, 0, i)

    out = np.array([500], dtype=dtype)
    do_all(range(1000), f(out), steal=False)
    assert out[0] == 0
