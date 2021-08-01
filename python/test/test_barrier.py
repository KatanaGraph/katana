import numpy as np
import pytest

from katana import do_all, do_all_operator
from katana.galois import get_active_threads, set_active_threads
from katana.local import SimpleBarrier, get_fast_barrier


def test_fast_barrier_invalidate():
    threads = set_active_threads(2)
    if threads != 2:
        pytest.skip("This test requires at least 2 threads.")

    b1 = get_fast_barrier(1)
    assert get_fast_barrier(1) is b1
    b1.reset()

    b2 = get_fast_barrier(2)
    assert b2 is not b1
    b2.reset()
    with pytest.raises(ValueError):
        b1.reset()
    b2.reset()


def test_fast_barrier(threads_many):
    # pylint: disable=unused-argument
    barrier = get_fast_barrier()
    out = []

    def op(v):
        out.append(v)
        barrier.wait()
        out.append(v)

    threads = get_active_threads()
    do_all(range(threads), op)
    assert set(out[:threads]) == set(range(threads))
    assert set(out[threads:]) == set(range(threads))


def test_fast_barrier_in_numba(threads_many):
    # pylint: disable=unused-argument
    barrier = get_fast_barrier()
    threads = get_active_threads()
    a = np.zeros(threads, dtype=int)
    b = np.zeros(threads, dtype=int)

    @do_all_operator()
    def op(a, b, i):
        a[i] = 1
        barrier.wait()
        b[i] = a.sum()

    do_all(range(threads), op(a, b))
    assert np.all(a == np.ones(threads))
    assert np.all(b == np.ones(threads) * threads)


# TODO(amp): Reenable when SimpleBarrier is fixed. It is broken and deadlocks.
@pytest.mark.skip("Simple barrier is broken in C++.")
def test_simple_barrier(threads_many):
    # pylint: disable=unused-argument
    threads = get_active_threads()
    barrier = SimpleBarrier(threads)
    out = []

    def op(v):
        out.append(v)
        barrier.wait()
        out.append(v)

    do_all(range(threads), op)
    assert set(out[:threads]) == set(range(threads))
    assert set(out[threads:]) == set(range(threads))
