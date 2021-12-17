from katana import url


def test_url_join_path():
    assert url.URL._join_path("file:///home/", "a", "b") == "file:///home/a/b"
    assert url.URL._join_path("file:///home?query=string", "a") == "file:///home/a?query=string"
    assert url.URL._join_path("file:///home", "a") == "file:///home/a"
    assert url.URL._join_path("file:///home/", "a") == "file:///home/a"
    assert url.URL._join_path("file:///home/", "a/") == "file:///home/a/"
    assert url.URL._join_path("file:///home/") == "file:///home/"


def test_url_div_op():
    assert str(url.URL("file:///home/") / "a" / "b") == "file:///home/a/b"
    assert str(url.URL("file:///home/") / "a/b") == "file:///home/a/b"
    assert str(url.URL("file:///home?query=string") / "a") == "file:///home/a?query=string"
    assert url.URL("file:///home") / "a" == url.URL("file:///home/a")
    assert url.URL("file:///home/") / "a" == url.URL("file:///home/a")
    assert str("file:///home/" / url.URL("a/")) == "file:///home/a/"
    assert str(url.URL("file:///home/")) == "file:///home/"
    assert str(url.URL("a") / "b.txt") == "a/b.txt"
