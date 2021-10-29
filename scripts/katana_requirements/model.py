from abc import abstractmethod
from enum import Enum
from typing import Collection, Dict, FrozenSet, Generator, Iterator, List, Optional, Union

from packaging.version import Version


# TODO(amp): Remove special cases for these throughout the system. This should probably be configurable.
class OutputFormat(Enum):
    YAML = "yaml"
    CMAKE = "cmake"
    CONDA = "conda"
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


class VersionRequirement(Mergeable):
    lower_bound: Version
    upper_bound: Optional[Version]

    def __init__(self, lower_bound, upper_bound):
        self.lower_bound = lower_bound
        self.upper_bound = upper_bound

    def __hash__(self):
        return hash((self.lower_bound, self.upper_bound))

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
        if format in (OutputFormat.CONDA, OutputFormat.PIP):
            upper_bound = f",<{self.upper_bound}a0" if self.upper_bound else ""
            return f""">={self.lower_bound}{upper_bound}"""
        if format == OutputFormat.APT:
            upper_bound = f",<<{self.upper_bound}" if self.upper_bound else ""
            return f""">={self.lower_bound}{upper_bound}"""
        if format == OutputFormat.CONAN:
            return f"""{self.lower_bound}"""
        if format == OutputFormat.CMAKE:
            upper_bound = f"""...<{self.upper_bound}""" if self.upper_bound else ""
            return f"""{self.lower_bound}{upper_bound}"""
        raise ValueError(format)

    @classmethod
    def from_dict(cls, data) -> "VersionRequirement":
        return cls(
            Version(data[0]),
            Version(data[1])
            if len(data) > 1 and data[1] not in VersionRequirement.UNKNOWN_UPPER_BOUND_STRINGS
            else None,
        )


class Package(Mergeable):
    name: str
    version: VersionRequirement
    labels: FrozenSet[str]
    version_overrides: Dict[str, str]
    name_overrides: Dict[str, str]

    def __init__(self, name, version, labels, version_overrides, name_overrides):
        self.name = name
        self.version = version
        self.labels = labels
        self.version_overrides = version_overrides
        self.name_overrides = name_overrides

    def __hash__(self):
        return hash((self.name, self.version, self.labels))

    def name_for(self, format: OutputFormat) -> str:
        packaging_system = format.value
        return self.name_overrides.get(packaging_system, self.name)

    def version_for(self, format: OutputFormat) -> str:
        packaging_system = format.value
        return self.version_overrides.get(packaging_system, self.version.format(format))

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
        name = self.name_for(format)
        version_str = self.version_for(format)

        if format == OutputFormat.YAML:
            labels_str = ", ".join(self.labels)
            return (
                f"""{name}: {{"version": {version_str}, "labels": [{labels_str}], """
                f""""version_overrides": {self.version_overrides}, "name_overrides": {self.name_overrides} }}"""
            )
        if format == OutputFormat.CONAN:
            return f"""{name}/{version_str}"""
        if format == OutputFormat.CMAKE:
            name = name.upper().replace("-", "_")
            return f"""set({name}_VERSION {version_str})"""
        if format == OutputFormat.APT:
            # The deb version spec language only allows one version restriction per package name. So we list the
            # package twice, once for the lower-bound, once for the upper-bound.
            # TODO(amp): Splitting the string here is bad, there should be some way to get the exact parts of the
            #  version we want.
            vs = version_str.split(",")
            return ", ".join(f"{name} ({v})" for v in vs)
        if format in (OutputFormat.PIP, OutputFormat.CONDA):
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


class Requirements(Mergeable):
    labels: Dict[str, str]
    packaging_systems: Dict[str, str]
    packages: List[Package]

    def __init__(self, labels, packaging_systems, packages):
        self.labels = labels
        self.packaging_systems = packaging_systems
        self.packages = packages

    def packages_dict(self, format: OutputFormat = OutputFormat.YAML) -> Dict[str, Package]:
        return {p.name_for(format): p for p in self.packages}

    def select_packages(
        self, labels: Collection[str] = (), packaging_system: Union[str, OutputFormat] = None
    ) -> Iterator[Package]:
        labels_to_find = frozenset(labels)

        if hasattr(packaging_system, "value"):
            packaging_system = packaging_system.value

        for p in self.packages:
            package_disabled = p.name_overrides.get(packaging_system, True) is None
            has_specified_label = p.labels & labels_to_find
            labels_provided = labels_to_find
            if (not labels_provided or has_specified_label) and not package_disabled:
                yield p

    def merge(self, other: "Requirements") -> "Requirements":
        return Requirements(
            merge_dicts(self.labels, other.labels),
            merge_dicts(self.packaging_systems, other.packaging_systems),
            list(merge_dicts(self.packages_dict(), other.packages_dict()).values()),
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
