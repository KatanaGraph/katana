import pytest

from katana.dynamic_bitset import DynamicBitset

SIZE = 50

@pytest.fixture
def dbs():
    bs = DynamicBitset()
    bs.resize(SIZE)
    return bs

def test_empty(dbs):
    pass

def test_set(dbs):
    dbs.set(10)
    assert dbs.test(10)

def test_reset(dbs):
    dbs.set(10)
    dbs.reset()
    assert not dbs.test(10)

def test_reset_index(dbs):
    dbs.set(10)
    dbs.reset_index(10)
    assert not dbs.test(10)

def test_resize(dbs):
   dbs.resize(20)
   assert dbs.size() == 20

   dbs.set(8)
   dbs.resize(20)
   assert dbs.size() == 20
   assert dbs.test(8)

   dbs.resize(70)
   assert dbs.size() == 70
   assert dbs.test(8)
   assert dbs.count() == 1

def test_clear(dbs):
    dbs.set(10)
    dbs.clear()
    assert dbs.size() == 0
    dbs.resize(20)
    assert dbs.size() == 20
    assert not dbs.test(10)

def test_reset_begin_end(dbs):
    dbs.set(10)
    dbs.set(15)
    dbs.reset_begin_end(12, 17)
    assert dbs.test(10)
    assert not dbs.test(15)

def test_count(dbs):
    dbs.set(10)
    assert dbs.count() == 1
