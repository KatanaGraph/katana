# content of tests/test_top.py
import pytest

@pytest.fixture
def innermost(order):
    order.append("innermost top")

def test_order(order, top):
    # print ('fuck from insomniapx')
    assert order == ["innermost top", "top"]


def test_order2(order):
    # print ('fuck from insomniapx')
    assert order == []


# tests/
#     __init__.py

#     conftest.py
#         # content of tests/conftest.py
#         import pytest

#         @pytest.fixture
#         def order():
#             return []

#         @pytest.fixture
#         def top(order, innermost):
#             order.append("top")

#     test_top.py
#         # content of tests/test_top.py
#         import pytest

#         @pytest.fixture
#         def innermost(order):
#             order.append("innermost top")

#         def test_order(order, top):
#             assert order == ["innermost top", "top"]

#     subpackage/
#         __init__.py

#         conftest.py
#             # content of tests/subpackage/conftest.py
#             import pytest

#             @pytest.fixture
#             def mid(order):
#                 order.append("mid subpackage")

#         test_subpackage.py
#             # content of tests/subpackage/test_subpackage.py
#             import pytest

#             @pytest.fixture
#             def innermost(order, mid):
#                 order.append("innermost subpackage")

#             def test_order(order, top):
#                 assert order == ["mid subpackage", "innermost subpackage", "top"]