from pathlib import Path
from typing import Collection, List, Optional, Union

import yaml

from .model import OutputFormat, Requirements

KATANA_REQUIREMENTS_FILE_NAME = "katana_requirements.yaml"
DEFAULT_DATA_SOURCE = [
    # Enterprise requirements file
    Path(__file__).absolute().parent.parent.parent.parent.parent / KATANA_REQUIREMENTS_FILE_NAME,
    # Open requirements file
    Path(__file__).absolute().parent.parent.parent / KATANA_REQUIREMENTS_FILE_NAME,
]


def load(input_files: Optional[Collection[Union[str, Path]]] = None) -> (Requirements, List[Path]):
    input_options = [Path(f) for f in input_files or DEFAULT_DATA_SOURCE]
    inputs = [f for f in input_options if f.exists()]
    data: Optional[Requirements] = None
    for input in reversed(inputs):
        # Use BaseLoader so everything is loaded as a string.
        d = Requirements.from_dict(yaml.load(open(input, "r", encoding="UTF-8"), Loader=yaml.BaseLoader))

        if data:
            data = data.merge(d)
        else:
            data = d
    return data, inputs


def package_list(labels: Collection[str], format: OutputFormat) -> List[str]:
    data, _ = load()
    return [p.format(data.packaging_systems[format.value]) for p in data.select_packages(labels, format)]


def package(name: str, format: OutputFormat) -> str:
    data, _ = load()
    return data.packages_dict()[name].format(data.packaging_systems[format.value])
