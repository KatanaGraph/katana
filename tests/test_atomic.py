import pytest
import numpy as np

from galois.loops import *
from galois.atomic import *

types = [
    pytest.param(int, id="int"),
    pytest.param(float, id="float"),
    pytest.param('uint64_t', id="uint64_t"),
]

acc_types = [
    pytest.param(GAccumulator, 15, id="GAccumulator"),
    pytest.param(GReduceMax, 10, id="GReduceMax"),
    pytest.param(GReduceMin, 2, id="GReduceMin"),
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

def test_GAccumulator_parallel():
    T = GAccumulator[int]
    acc = T()
    @do_all_operator()
    def f(acc, i):
        acc.update(i)
    do_all(range(1000), f(acc), steal=False)
    assert acc.reduce() == 499500

def test_GReduceMax_parallel():
    T = GReduceMax[int]
    acc = T()
    @do_all_operator()
    def f(acc, i):
        acc.update(abs(500 - i))
    do_all(range(1000), f(acc), steal=False)
    assert acc.reduce() == 500

def test_GReduceMin_parallel():
    T = GReduceMin[float]
    acc = T()
    @do_all_operator()
    def f(acc, i):
        acc.update((i - 500) / 10)
    do_all(range(1000), f(acc), steal=False)
    assert acc.reduce() == -50.0

def test_GReduceLogicalOr_parallel():
    T = GReduceLogicalOr
    acc = T()
    @do_all_operator()
    def f(acc, i):
        acc.update(i % 3 == 0)
    do_all(range(1000), f(acc), steal=False)
    assert acc.reduce() == True

def test_GReduceLogicalAnd_parallel():
    T = GReduceLogicalAnd
    acc = T()
    @do_all_operator()
    def f(acc, i):
        acc.update(i % 3 == 0)
    do_all(range(1000), f(acc), steal=False)
    assert acc.reduce() == False

def test_GReduceLogicalOr_simple():
    T = GReduceLogicalOr
    acc = T()
    acc.update(True)
    acc.update(False)
    acc.update(True)
    assert acc.reduce() == True

def test_GReduceLogicalAnd_simple():
    T = GReduceLogicalAnd
    acc = T()
    acc.update(True)
    acc.update(False)
    acc.update(True)
    assert acc.reduce() == False