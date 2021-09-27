import zipfile
from tempfile import NamedTemporaryFile


def test_bug_capture_environment():
    import katana.bug

    with NamedTemporaryFile(suffix=".zip", delete=False) as fi:
        katana.bug.capture_environment(fi)

        # Check that the scripts captured version information in both environments as a general check.
        with zipfile.ZipFile(fi) as zipin:
            assert katana.__version__.encode("utf-8") in zipin.open("info/katana.txt").read()
