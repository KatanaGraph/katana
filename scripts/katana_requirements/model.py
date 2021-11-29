import itertools
from abc import abstractmethod
from enum import Enum
from typing import Collection, Dict, FrozenSet, Iterator, List, Optional, Union

from packaging.version import Version


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


def unique_sequence(s):
    return type(s)({k: True for k in s}.keys())


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

    def name_for(self, ps: "PackagingSystem") -> str:
        if ps.name in self.name_overrides:
            return self.name_overrides[ps.name]
        return self.name

    def version_for(self, ps: "PackagingSystem") -> str:
        if ps.name in self.version_overrides:
            return self.version_overrides[ps.name]
        return self.version.format(ps.format)

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

    def format(self, ps: "PackagingSystem") -> str:
        format = ps.format
        name = self.name_for(ps)
        version_str = self.version_for(ps)

        if name is None:
            raise ValueError(f"Missing name for {self.name} in packaging system {ps}")
        if version_str is None:
            raise ValueError(f"Missing version for {self.name} in packaging system {ps}")

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


class Label(Mergeable):
    name: str
    inherits: List[str]
    description: str

    def __init__(self, name, inherits, description):
        self.name = name
        self.inherits = list(inherits)
        self.description = description

    def merge(self, other: "Label") -> "Label":
        return Label(self.name, unique_sequence(self.inherits + other.inherits), self.description)

    def __eq__(self, other):
        return self.name == other.name

    def __hash__(self):
        return hash(self.name)

    def __repr__(self):
        return self.name

    @classmethod
    def from_dict(cls, name, data):
        return cls(name, data.get("inherits", []), data.get("description", ""))


class PackagingSystem(Mergeable):
    name: str
    inherits: List[str]
    description: str
    format: OutputFormat
    channels: List[str]

    def __init__(self, name, inherits, description, format, channels):
        self.name = name
        self.inherits = list(inherits)
        self.description = description
        self.format = OutputFormat(format)
        self.channels = list(channels)

    def merge(self, other: "PackagingSystem") -> "PackagingSystem":
        return PackagingSystem(
            self.name,
            unique_sequence(self.inherits + other.inherits),
            self.description,
            self.format,
            unique_sequence(self.channels + other.channels),
        )

    def __eq__(self, other):
        return self.name == other.name

    def __hash__(self):
        return hash(self.name)

    def __repr__(self):
        return self.name

    @classmethod
    def from_dict(cls, name, data):
        return cls(
            name,
            data.get("inherits", []),
            data.get("description", ""),
            data.get("format", ""),
            data.get("channels", []),
        )


class Requirements(Mergeable):
    labels: Dict[str, Label]
    packaging_systems: Dict[str, PackagingSystem]
    packages: List[Package]

    def __init__(self, labels, packaging_systems, packages):
        self.labels = labels
        self.packaging_systems = packaging_systems
        self.packages = [self._add_supers(p) for p in packages]

    def packages_dict(self, ps: Optional[PackagingSystem] = None) -> Dict[str, Package]:
        return {p.name_for(ps): p for p in self.select_packages(packaging_system=ps)}

    def _all_supers(self, sub, name_map):
        ret = [sub]
        for u_name in sub.inherits:
            u = name_map[u_name]
            ret.extend(self._all_supers(u, name_map))
        return ret

    def _all_subs(self, super, name_map):
        ret = [super]
        for v in name_map.values():
            if super.name in v.inherits:
                ret.extend(self._all_subs(v, name_map))
        return ret

    def _super_labels(self, sub: Label):
        return self._all_supers(sub, self.labels)

    def _sub_labels(self, super: Label):
        return self._all_subs(super, self.labels)

    def _sub_packaging_systems(self, sub: PackagingSystem):
        return self._all_subs(sub, self.packaging_systems)

    def _add_supers(self, p: Package):
        # Expand the labels to include all *super* labels. We use super here because labels represent membership.
        new_labels = {l.name for sub_label_name in p.labels for l in self._sub_labels(self.labels[sub_label_name])}

        # Expand the packaging systems to include all *sub* packaging systems. We use sub here because packaging
        # systems represent specialized behavior.
        new_name_overrides = dict(p.name_overrides)
        for ps_name, override in p.name_overrides.items():
            for sub_ps in self._sub_packaging_systems(self.packaging_systems[ps_name]):
                new_name_overrides.setdefault(sub_ps.name, override)

        new_version_overrides = dict(p.version_overrides)
        for ps_name, override in p.version_overrides.items():
            for sub_ps in self._sub_packaging_systems(self.packaging_systems[ps_name]):
                new_version_overrides.setdefault(sub_ps.name, override)

        return Package(p.name, p.version, new_labels, new_version_overrides, new_name_overrides)

    def select_packages(
        self, labels: Collection[str] = (), packaging_system: Union[str, OutputFormat, PackagingSystem] = None
    ) -> Iterator[Package]:
        labels_to_find = frozenset(labels)

        if hasattr(packaging_system, "value"):
            packaging_system = packaging_system.value
        if isinstance(packaging_system, str):
            packaging_system = self.packaging_systems[packaging_system]

        for p in self.packages:
            package_disabled = p.name_for(packaging_system) is None
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
        labels = {k: Label.from_dict(k, d) for k, d in data.get("labels", {}).items()}
        packaging_systems = {k: PackagingSystem.from_dict(k, d) for k, d in data.get("packaging_systems", {}).items()}
        packages = [Package.from_dict(k, d) for k, d in data.items() if k not in ("labels", "packaging_systems")]

        for p in packages:
            for l in p.labels:
                labels.setdefault(l, Label(l, [], ""))
            for ps in itertools.chain(p.name_overrides.keys(), p.version_overrides.keys()):
                packaging_systems.setdefault(ps, PackagingSystem(ps, [], "", OutputFormat.YAML, []))
        return cls(labels, packaging_systems, packages)
