import pytest

from katana.dynamic_bitset import DynamicBitset

__all__ = []

SIZE = 50


@pytest.fixture
def dbs():
    bs = DynamicBitset()
    bs.resize(SIZE)
    return bs


def test_set(dbs):
    dbs[10] = 1
    assert dbs[10]


def test_set_invalid_value(dbs):
    try:
        dbs[10] = 111
        assert False
    except AttributeError:
        pass


def test_set_invalid_type(dbs):
    try:
        dbs[2.3] = 0
        assert False
    except TypeError:
        pass


def test_set_invalid_index_low(dbs):
    try:
        dbs[-1] = 1
        assert False
    except IndexError:
        pass


def test_set_invalid_index_high(dbs):
    try:
        dbs[SIZE] = 1
        assert False
    except IndexError:
        pass


def test_reset(dbs):
    dbs[10] = 1
    dbs.reset()
    assert not dbs[10]
    assert len(dbs) == SIZE


def test_reset_index(dbs):
    dbs[10] = 1
    dbs[10] = 0
    assert not dbs[10]


def test_reset_begin_end(dbs):
    dbs[10] = 1
    dbs[15] = 1
    dbs[12:17] = 0
    assert dbs[10]
    assert not dbs[15]


def test_reset_begin_end_invalid_step(dbs):
    try:
        dbs[12:17:22] = 0
        assert False
    except AttributeError:
        pass


def test_reset_none_end(dbs):
    dbs[10] = 1
    dbs[15] = 1
    dbs[:12] = 0
    assert not dbs[10]
    assert dbs[15]


def test_resize(dbs):
    dbs.resize(20)
    assert len(dbs) == 20

    dbs[8] = 1
    dbs.resize(20)
    assert len(dbs) == 20
    assert dbs[8]

    dbs.resize(70)
    assert len(dbs) == 70
    assert dbs[8]
    assert dbs.count() == 1


def test_clear(dbs):
    dbs[10] = 1
    dbs.clear()
    assert len(dbs) == 0
    dbs.resize(20)
    assert len(dbs) == 20
    assert not dbs[10]


def test_count(dbs):
    dbs[10] = 1
    assert dbs.count() == 1
