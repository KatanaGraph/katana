import sys
import traceback

try:
    import packaging
    import yaml
except ImportError:
    traceback.print_exc()
    print()
    print("ERROR: The python packages pyyaml and packaging must be available to run this script.")
    print("To install them in conda: conda install pyyaml packaging")
    print("To install them on Ubuntu: apt install python3-yaml python3-packaging")
    sys.exit(1)

from .data import load, package, package_list
from .model import OutputFormat, Package, Requirements
