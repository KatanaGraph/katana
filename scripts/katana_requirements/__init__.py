from abc import abstractmethod
from dataclasses import dataclass, field
from enum import Enum
from typing import Dict, FrozenSet, List, Optional

from packaging.version import Version


# TODO(amp): Remove special cases for these throughout the system. This should probably be configurable.
class OutputFormat(Enum):
    YAML = "yaml"
    CMAKE = "cmake"
    CONDA = "conda"
    CONDA_CMD_LINE = "conda_cmd_line"
    CONAN = "conan"
    APT = "apt"
    PIP = "pip"


def merge_dicts(a: dict, b: dict) -> dict:
    ret = dict(a)
    for k, v in b.items():
        if k in ret and isinstance(ret[k], Mergeable):
            ret[k] = ret[k].merge(v)
        else:
            ret[k] = v
    return ret


def convert_nulls(d: dict) -> dict:
    return {k: (None if v == "null" else v) for k, v in d.items()}


class Mergeable:
    @abstractmethod
    def merge(self, other: "Mergeable") -> "Mergeable":
        raise NotImplementedError()


@dataclass(unsafe_hash=True, frozen=True)
class VersionRequirement(Mergeable):
    lower_bound: Version
    upper_bound: Optional[Version]

    UNKNOWN_UPPER_BOUND_STRINGS = frozenset({"null", r"¯\(ツ)/¯", "", "None"})

    def merge(self, other: "VersionRequirement") -> "VersionRequirement":
        if self.upper_bound and other.upper_bound:
            upper_bound = min(self.upper_bound, other.upper_bound)
        elif self.upper_bound:
            upper_bound = self.upper_bound
        else:
            upper_bound = other.upper_bound
        return VersionRequirement(max(self.lower_bound, other.lower_bound), upper_bound)

    def format(self, format: OutputFormat) -> str:
        if format == OutputFormat.YAML:
            return f"""["{self.lower_bound}", "{self.upper_bound or 'null'}"]"""
        if format in (OutputFormat.CONDA, OutputFormat.CONDA_CMD_LINE, OutputFormat.PIP):
            upper_bound = f",<{self.upper_bound}a0" if self.upper_bound else ""
            return f""">={self.lower_bound}{upper_bound}"""
        if format == OutputFormat.APT:
            upper_bound = f", <<{self.upper_bound}" if self.upper_bound else ""
            return f""">={self.lower_bound}{upper_bound}"""
        if format == OutputFormat.CONAN:
            return f"""{self.lower_bound}"""
        if format == OutputFormat.CMAKE:
            upper_bound = f"""...<{self.upper_bound}""" if self.upper_bound else ""
            return f"""{self.lower_bound}{upper_bound}"""
        raise ValueError(format)

    @classmethod
    def from_dict(cls, data: dict) -> "VersionRequirement":
        return cls(
            Version(data[0]),
            Version(data[1])
            if len(data) > 1 and data[1] not in VersionRequirement.UNKNOWN_UPPER_BOUND_STRINGS
            else None,
        )


@dataclass(unsafe_hash=True, frozen=True)
class Package(Mergeable):
    name: str
    version: VersionRequirement
    labels: FrozenSet[str]
    version_overrides: Dict[str, str] = field(hash=False)
    name_overrides: Dict[str, str] = field(hash=False)

    def merge(self, other: "Package") -> "Package":
        if self.name != other.name:
            raise ValueError(f"Name mismatch: {self.name} != {other.name}")
        return Package(
            self.name,
            self.version.merge(other.version),
            self.labels | other.labels,
            merge_dicts(self.version_overrides, other.version_overrides),
            merge_dicts(self.name_overrides, other.name_overrides),
        )

    def format(self, format: OutputFormat) -> str:
        if format == OutputFormat.CONDA_CMD_LINE:
            packaging_system = "conda"
        else:
            packaging_system = format.value
        name = self.name_overrides.get(packaging_system, self.name)
        version_str = self.version_overrides.get(packaging_system, self.version.format(format))

        if format == OutputFormat.YAML:
            labels_str = ", ".join(self.labels)
            return (
                f"""{name}: {{"version": {version_str}, "labels": [{labels_str}], """
                f""""version_overrides": {self.version_overrides}, "name_overrides": {self.name_overrides} }}"""
            )
        if format == OutputFormat.CONDA:
            return f"""{name}: {version_str}"""
        if format == OutputFormat.CONAN:
            return f"""{name}/{version_str}"""
        if format == OutputFormat.CMAKE:
            name = name.upper().replace("-", "_")
            return f"""set({name}_VERSION {version_str})"""
        if format == OutputFormat.APT:
            return f"""{name} ({version_str})"""
        if format in (OutputFormat.PIP, OutputFormat.CONDA_CMD_LINE):
            return f"""{name}{version_str}"""
        raise ValueError(format)

    @classmethod
    def from_dict(cls, name: str, data: dict) -> "Package":
        return cls(
            name,
            VersionRequirement.from_dict(data["version"]),
            frozenset(data.get("labels", ())),
            data.get("version_overrides", {}),
            convert_nulls(data.get("name_overrides", {})),
        )


@dataclass(unsafe_hash=True, frozen=True)
class Requirements(Mergeable):
    labels: Dict[str, str] = field(hash=False)
    packaging_systems: Dict[str, str] = field(hash=False)
    packages: List[Package]

    @property
    def packages_dict(self):
        return {p.name: p for p in self.packages}

    def merge(self, other: "Requirements") -> "Requirements":
        return Requirements(
            merge_dicts(self.labels, other.labels),
            merge_dicts(self.packaging_systems, other.packaging_systems),
            list(merge_dicts(self.packages_dict, other.packages_dict).values()),
        )

    @classmethod
    def from_dict(cls, data: dict) -> "Requirements":
        return cls(
            convert_nulls(data.get("label_descriptions", {})),
            convert_nulls(data.get("packaging_system_descriptions", {})),
            list(
                Package.from_dict(k, d)
                for k, d in data.items()
                if k not in ("label_descriptions", "packaging_system_descriptions")
            ),
        )
