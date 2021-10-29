import weakref
from functools import partial

import numba
import numpy as np
import pytest
from numba import from_dtype

from katana import (
    OrderedByIntegerMetric,
    PerSocketChunkFIFO,
    do_all,
    do_all_operator,
    for_each,
    for_each_operator,
    obim_metric,
)

simple_modes = [
    pytest.param(dict(steal=True), id="steal=True"),
    # pytest.param(dict(steal=False), id="steal=False"),
    pytest.param(dict(), id="{}"),
    pytest.param(dict(loop_name="test_loop_name"), id="loop_name"),
]
no_conflicts_modes = [
    pytest.param(dict(no_pushes=True), id="no_pushes=True"),
    pytest.param(dict(no_pushes=False), id="no_pushes=False"),
]
types = [
    pytest.param(int, id="int"),
    pytest.param(float, id="float"),
    pytest.param(np.uint64, id="uint64_t"),
    pytest.param(np.int32, id="int32_t"),
]


@pytest.mark.parametrize("modes", simple_modes)
def test_do_all_python(modes):
    total = 0

    def f(i):
        nonlocal total
        total += i

    do_all(range(10), f, **modes)
    assert total == 45


@pytest.mark.parametrize("modes", simple_modes)
def test_for_each_python_with_push(modes):
    total = 0

    def f(i, ctx):
        nonlocal total
        total += i
        if i == 8:
            ctx.push(100)

    for_each(range(10), f, **modes)
    assert total == 145


def test_for_each_conflict_detection_unsupported():
    total = 0

    def f(i, ctx):
        _ = ctx
        nonlocal total
        total += i

    with pytest.raises(ValueError):
        for_each(range(10), f, disable_conflict_detection=False)


@pytest.mark.xfail(numba.config.DISABLE_JIT, reason="Closures are not type checked in non-jit runs.")
def test_for_each_wrong_closure():
    @do_all_operator()
    def f(out, i):
        out[i] = i + 1

    out = np.zeros(10, dtype=int)
    with pytest.raises(TypeError):
        for_each(range(10), f(out))


@pytest.mark.xfail(numba.config.DISABLE_JIT, reason="Closures are not type checked in non-jit runs.")
def test_do_all_wrong_closure():
    @for_each_operator()
    def f(out, i, ctx):
        _ = ctx
        out[i] = i + 1

    out = np.zeros(10, dtype=int)
    with pytest.raises(TypeError):
        do_all(range(10), f(out))


@pytest.mark.parametrize("modes", simple_modes)
def test_do_all(modes):
    @do_all_operator()
    def f(out, i):
        out[i] = i + 1

    out = np.zeros(10, dtype=int)
    do_all(range(10), f(out), **modes)
    assert np.allclose(out, np.array(range(1, 11)))


@pytest.mark.parametrize("modes", simple_modes)
def test_do_all_opaque(modes):
    from katana.local import InsertBag

    @do_all_operator()
    def f(out, s):
        out[s.y] = s.x

    dt = np.dtype([("x", np.float32), ("y", np.int8),], align=True)
    data = InsertBag[dt]()
    data.push((1.1, 0))
    data.push((2.1, 1))
    data.push((3.1, 3))

    out = np.zeros(4, dtype=float)
    do_all(data, f(out), **modes)
    assert np.allclose(out, np.array([1.1, 2.1, 0, 3.1]))


@pytest.mark.parametrize("modes", simple_modes)
@pytest.mark.parametrize("typ", types)
def test_do_all_specific_type(modes, typ):
    from katana.local import InsertBag

    @do_all_operator()
    def f(out, i):
        out[int(i)] = i

    data = InsertBag[typ]()
    for i in range(1000):
        data.push(i)

    out = np.zeros(1000, dtype=typ)
    do_all(data, f(out), **modes)
    assert np.allclose(out, np.array(range(1000)))
    if not numba.config.DISABLE_JIT:
        # Check that the operator was actually compiled for the correct type
        # [0][1][0] = [first overload][second argument][first possible type]
        # I'm not sure why the last indexing is used. It always seems to be a 1-tuple, but numba makes it. *shrug*
        assert list(f.inspect_llvm().keys())[0][1][0] == from_dtype(np.dtype(typ))


@pytest.mark.parametrize("modes", simple_modes + no_conflicts_modes)
def test_for_each_no_push(modes):
    @for_each_operator()
    def f(out, i, ctx):
        _ = i
        _ = ctx
        out[i] += i + 1

    out = np.zeros(10, dtype=int)
    for_each(range(10), f(out), **modes)
    assert np.allclose(out, np.array([1, 2, 3, 4, 5, 6, 7, 8, 9, 10]))


@pytest.mark.parametrize("modes", simple_modes)
def test_for_each(modes):
    @for_each_operator()
    def f(out, i, ctx):
        out[i] += i + 1
        if i == 8:
            ctx.push(10)

    out = np.zeros(11, dtype=int)
    for_each(range(10), f(out), **modes)
    assert np.allclose(out, np.array([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11]))


@pytest.mark.parametrize("modes", simple_modes)
def test_for_each_opaque(modes):
    from katana.local import InsertBag

    @for_each_operator()
    def f(out, s, ctx):
        out[s.y] += s.x
        if s.y < 10:
            ctx.push((s.x + 1, s.y + 1))

    dt = np.dtype([("x", np.float32), ("y", np.int8),], align=True)
    data = InsertBag[dt]()
    data.push((1.1, 0))

    out = np.zeros(10, dtype=float)
    for_each(data, f(out), **modes)
    assert np.allclose(out, np.array([1.1, 2.1, 3.1, 4.1, 5.1, 6.1, 7.1, 8.1, 9.1, 10.1]))


def test_obim_python(threads_1):
    _ = threads_1

    order = []

    def metric(out, i):
        return out[i]

    def f(out, i, ctx):
        order.append(i)
        orig = out[i]
        out[i] = 10 - i
        if orig == 0:
            ctx.push(i)

    out = np.zeros(10, dtype=int)
    for_each(
        range(10), partial(f, out), worklist=OrderedByIntegerMetric(partial(metric, out)),
    )
    assert np.allclose(out, np.array([10, 9, 8, 7, 6, 5, 4, 3, 2, 1]))
    assert order == [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 9, 8, 7, 6, 5, 4, 3, 2, 1, 0]


def test_obim(threads_1):
    _ = threads_1

    order = []

    @obim_metric()
    def metric(out, i):
        return out[i]

    def f(out, i, ctx):
        order.append(i)
        orig = out[i]
        out[i] = 10 - i
        if orig == 0:
            ctx.push(i)

    out = np.zeros(10, dtype=int)
    for_each(range(10), partial(f, out), worklist=OrderedByIntegerMetric(metric(out)))
    assert np.allclose(out, np.array([10, 9, 8, 7, 6, 5, 4, 3, 2, 1]))
    assert order == [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 9, 8, 7, 6, 5, 4, 3, 2, 1, 0]


def test_per_socket_chunk_fifo(threads_1):
    _ = threads_1

    order = []

    def f(out, i, ctx):
        order.append(i)
        orig = out[i]
        out[i] = 10 - i
        if orig == 0:
            ctx.push(9 - i)

    out = np.zeros(10, dtype=int)
    for_each(range(10), partial(f, out), worklist=PerSocketChunkFIFO())
    assert np.allclose(out, np.array([10, 9, 8, 7, 6, 5, 4, 3, 2, 1]))
    assert order == [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 9, 8, 7, 6, 5, 4, 3, 2, 1, 0]


def test_closure_memory_management():
    @do_all_operator()
    def f(x, y):
        _ = x
        _ = y

    a = np.zeros((100,))
    w = weakref.ref(a)
    c = f(a)
    del a
    assert w() is not None
    del c
    assert w() is None
