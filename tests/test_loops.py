import weakref
from functools import partial

import numpy as np
import pytest

from galois.loops import *

simple_modes = [
    pytest.param(dict(steal=True), id="steal=True"),
    pytest.param(dict(steal=False), id="steal=False"),
    pytest.param(dict(), id="{}"),
    pytest.param(dict(loop_name="test_loop_name"), id="loop_name"),
]
no_conflicts_modes = [
    pytest.param(dict(disable_conflict_detection=True), id="disable_conflict_detection=True"),
    pytest.param(dict(disable_conflict_detection=False), id="disable_conflict_detection=False"),
]


@pytest.mark.parametrize("modes", simple_modes)
def test_do_all_python(modes):
    total = 0

    def f(i):
        nonlocal total
        total += i

    do_all(range(10), f, **modes)
    assert total == 45


@pytest.mark.parametrize("modes", simple_modes + no_conflicts_modes)
def test_for_each_python_with_push(modes):
    total = 0

    def f(i, ctx):
        nonlocal total
        total += i
        if i == 8:
            ctx.push(100)

    for_each(range(10), f, **modes)
    assert total == 145


def test_for_each_wrong_closure():
    @do_all_operator()
    def f(out, i):
        out[i] = i + 1

    out = np.zeros(10, dtype=int)
    with pytest.raises(TypeError):
        for_each(range(10), f(out))


def test_do_all_wrong_closure():
    @for_each_operator()
    def f(out, i, ctx):
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


@pytest.mark.parametrize("modes", simple_modes + no_conflicts_modes)
def test_for_each(modes):
    @for_each_operator()
    def f(out, i, ctx):
        out[i] += i + 1
        if i == 8:
            ctx.push(1)

    out = np.zeros(10, dtype=int)
    for_each(range(10), f(out), **modes)
    assert np.allclose(out, np.array([1, 4, 3, 4, 5, 6, 7, 8, 9, 10]))


def test_obim_python(threads_1):
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
        pass

    a = np.zeros((100,))
    w = weakref.ref(a)
    c = f(a)
    del a
    assert w() is not None
    del c
    assert w() is None
