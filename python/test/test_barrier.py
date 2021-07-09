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
    _ = threads_many
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


# TODO(amp): Implement numba access for Barrier.
@pytest.mark.skip("Barriers not supported in numba code.")
def test_fast_barrier_in_numba(threads_many):
    _ = threads_many
    barrier = get_fast_barrier()
    out = []

    @do_all_operator()
    def op(v):
        out.append(v)
        barrier.wait()
        out.append(v)

    threads = get_active_threads()
    do_all(range(threads), op)
    assert set(out[:threads]) == set(range(threads))
    assert set(out[threads:]) == set(range(threads))


# TODO(amp): Reenable when SimpleBarrier is fixed. It is broken and deadlocks.
@pytest.mark.skip("Simple barrier is broken in C++.")
def test_simple_barrier(threads_many):
    _ = threads_many
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
