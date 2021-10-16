import argparse
import sys
from pathlib import Path

from katana.bug import capture_environment

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        prog=f"{Path(sys.executable).name} -m katana.bug",
        description="""
        Capture environment information for bug reporting.
        """,
    )

    parser.add_argument(
        "destination",
        help="Output the binary to the given path or stdout if '-' is given",
        type=str,
        nargs="?",
        default=None,
    )

    args = parser.parse_args()

    if args.destination == "-":
        destination = sys.stdout.buffer
    else:
        destination = args.destination

    filename = capture_environment(destination)
    if isinstance(filename, (str, Path)):
        print(f"Environment captured to: {filename}")
