from urllib import parse


class URL:
    """
    URL class that maintains semantics similar to pathlib. This class does not
    intend to act as a universal class for all path related things! Users can use
    the class in the following ways:

    - URL("file:///home/") / "a" / "b" => URL("file:///home/a/b")
    - "file:///home/" / URL("a") => URL("file:///home/a")
    """

    def __init__(self, url: str):
        self.url = url

    def __truediv__(self, other):
        return URL(self._join_path(self.url, other))

    def __rtruediv__(self, other):
        return URL(self._join_path(other, self.url))

    def __str__(self):
        return self.url

    def __eq__(self, other):
        return self.url == other.url

    @staticmethod
    def _join_path(url: str, *args) -> str:
        """
        _join_path joins a URL with some number of path components.

        In the returned URL, the path will be the path of the input URL joined with
        the additional path components. Each component will be separated by a "/".
        If the input URL already ends with a "/", no additional "/" will be added
        before the first path addition.

        All non-path components will be taken from the input URL.

        Examples:

        - _join_path("file:///home/", "a", "b") => "file:///home/a/b"
        - _join_path("file:///home?query=string", "a") => "file:///home/a?query=string"
        - _join_path("file:///home", "a") == join_path("file:///home/", "a")
        - _join_path("file:///home/") => "file:///home/"
        """
        if len(args) == 0:
            return url

        u = parse.urlparse(url)
        path = u.path
        if u.path.endswith("/"):
            path = u.path[:-1]
        paths = [path, *args]
        u = u._replace(path="/".join(paths))
        return u.geturl()
