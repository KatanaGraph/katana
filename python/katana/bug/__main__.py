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
        help="Output the binary to the given path, if this is not provided a temporary file name is chosen.",
        type=str,
        nargs="?",
        default=None,
    )

    parser.add_argument("--stdout", help="Send output to stdout instead of a file.", action="store_true")

    args = parser.parse_args()

    if args.stdout:
        if args.destination:
            print("WARNING: Ignoring filename since --stdout was given.", file=sys.stderr)
        destination = sys.stdout.buffer
    else:
        destination = args.destination

    filename = capture_environment(destination)
    if isinstance(filename, (str, Path)):
        print(f"Environment captured to: {filename}")
