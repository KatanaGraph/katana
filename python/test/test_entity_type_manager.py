import pytest

def test_node_types(graph):
    types = graph.node_type_names
    assert set(types) == {b'kUnknownName', b'Comment', b'Tag', b'City', b'University', b'Forum', b'Company', b'Continent', b'Country', b'Place', b'Organisation', b'TagClass', b'Person', b'Message', b'Post'}
