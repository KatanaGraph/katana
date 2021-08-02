import os
import tempfile
from pathlib import Path
from typing import Optional

# A temporary directory in which to write python code to exec. See exec_in_file.
TEMPORARY_DIRECTORY = None
if os.environ.get("KATANA_WRITE_CODE"):
    # pylint: disable=consider-using-with
    # This should last as long as the interpreter so don't use with.
    TEMPORARY_DIRECTORY_MANAGER = tempfile.TemporaryDirectory()
    TEMPORARY_DIRECTORY = Path(TEMPORARY_DIRECTORY_MANAGER.name)


def exec_in_file(filename: str, source: str, globals_dict: Optional[dict] = None, locals_dict: Optional[dict] = None):
    """
    Execute ``source`` just as `exec`. Also write the code to ``filename`` (in a temporary directory that is deleted at
    interpreter shutdown.) and associate the execution with that file.

    This improve debuggability because it allows the source to be found by the `inspect` module. This helps when
    debugging numba compilation problems.
    """
    source = source.strip()
    if not filename.endswith(".py"):
        filename += ".py"
    if TEMPORARY_DIRECTORY:
        filename = str(TEMPORARY_DIRECTORY / filename)
        with open(filename, "wt", encoding="UTF-8") as fi:
            print(source, file=fi)
    code = compile(source, filename, "exec")
    exec(code, globals_dict, locals_dict)
