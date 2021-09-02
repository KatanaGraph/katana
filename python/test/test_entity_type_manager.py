import pytest


def test_node_types(graph):
    types = graph.node_types
    assert {str(t) for t in types} == {
        "kUnknownName",
        "Comment",
        "Tag",
        "City",
        "University",
        "Forum",
        "Company",
        "Continent",
        "Country",
        "Place",
        "Organisation",
        "TagClass",
        "Person",
        "Message",
        "Post",
    }
