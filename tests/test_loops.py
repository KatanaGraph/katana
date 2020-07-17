import pytest
import numba
import numpy as np

from galois.loops import *

simple_modes = [
        pytest.param(dict(steal=True), id="steal=True"),
        pytest.param(dict(steal=False), id="steal=False"),
        pytest.param(dict(), id="{}"),
        pytest.param(dict(loop_name="test_loop_name"), id="loop_name")
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
    do_all(0, 10, f, **modes)
    assert total == 45

@pytest.mark.parametrize("modes", simple_modes + no_conflicts_modes)
def test_for_each_python_with_push(modes):
    total = 0
    def f(i, ctx):
        nonlocal total
        total += i
        if i == 8:
            ctx.push(100)
    for_each(0, 10, f, **modes)
    assert total == 145

def test_for_each_wrong_closure():
    @do_all_operator()
    def f(out, i):
        out[i] = i+1
    out = np.zeros(10, dtype=int)
    with pytest.raises(TypeError):
        for_each(0, 10, f(out))

def test_do_all_wrong_closure():
    @for_each_operator()
    def f(out, i, ctx):
        out[i] = i+1
    out = np.zeros(10, dtype=int)
    with pytest.raises(TypeError):
        do_all(0, 10, f(out))

@pytest.mark.parametrize("modes", simple_modes)
def test_do_all(modes):
    @do_all_operator()
    def f(out, i):
        out[i] = i+1
    out = np.zeros(10, dtype=int)
    do_all(0, 10, f(out), **modes)
    assert np.allclose(out, np.array(range(1, 11)))

@pytest.mark.parametrize("modes", simple_modes + no_conflicts_modes)
def test_for_each(modes):
    @for_each_operator()
    def f(out, i, ctx):
        out[i] += i+1
        if i == 8:
            ctx.push(1)
    out = np.zeros(10, dtype=int)
    for_each(0, 10, f(out), **modes)
    assert np.allclose(out, np.array([1, 4, 3, 4, 5, 6, 7, 8, 9, 10]))
