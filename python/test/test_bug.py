import zipfile
from tempfile import NamedTemporaryFile


def test_bug_capture_environment():
    import katana.bug

    with NamedTemporaryFile(suffix=".zip", delete=False) as fi:
        katana.bug.capture_environment(fi)

        with zipfile.ZipFile(fi) as zipin:
            assert katana.__version__.encode("utf-8") in zipin.open("info/katana.txt").read()
            files = {f.filename for f in zipin.filelist}
            for fn in ["info/os.txt", "info/python.txt", "info/conda.txt", "info/cmake.txt", "etc/ld.so.conf"]:
                assert fn in files
            # Check that we have a link.txt. This cannot be added above because the path varies with the build dir
            assert any("link.txt" in fn for fn in files)


def test_bug_capture_command_pass_invalid_command():
    from katana.bug.environment import capture_command

    # Make sure it doesn't fail for non existent command
    result = capture_command("non-existent-executable", "xyz")
    assert isinstance(result, str)
