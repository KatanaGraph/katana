import os
import subprocess
import sys
import tempfile
import traceback
from pathlib import Path
from typing import Optional, Set

import generate_from_jinja
import setuptools

__all__ = ["setup"]


def in_build_call():
    # This is a hack, but WOW setuptools is terrible.
    # We need to check if we are building because otherwise every call to setup.py (even for metadata not build) will
    # cause cython files to be processed. Often the processing happens in tree spewing build files all over the tree.
    return any(a.startswith("build") or a.startswith("install") or "dist" in a for a in sys.argv)


def split_cmake_list(s):
    return list(filter(None, s.split(";")))


def unique_list(l):
    return list(dict.fromkeys(l))


class RequirementError(RuntimeError):
    def __str__(self):
        return (
            super().__str__()
            + " (Normal Katana builds should use cmake to start the build and NOT directly call setup.py. "
            "cmake calls setup.py as needed. See docs/contributing/building.rst "
            "for Python build instructions.)"
        )


class RequirementsCache:
    """
    A simple set of strings that is synced to disk. It is used to check if a requirement has already succeeded.
    """

    cache_file: Optional[Path]
    cache: Set[str]

    def __init__(self):
        self.cache_file = os.environ.get("KATANA_SETUP_REQUIREMENTS_CACHE")
        if self.cache_file:
            self.cache_file = Path(self.cache_file)
            try:
                with open(self.cache_file, "rt", encoding="UTF-8") as fi:
                    self.cache = set(fi.readlines())
            except IOError:
                self.cache = set()
        else:
            self.cache_file = None
            self.cache = set()

    def sync(self):
        if self.cache_file:
            with open(self.cache_file, "wt", encoding="UTF-8") as fi:
                fi.writelines(l for l in self.cache)

    def __contains__(self, item):
        return self._make_key(item) in self.cache

    def add(self, *item):
        self.cache.add(self._make_key(item))
        self.sync()

    @classmethod
    def _make_key(cls, item):
        key = ";".join(repr(v) for v in item) + "\n"
        return key


requirement_cache = RequirementsCache()


def require_python_module(module_name, ge_version=None, lt_version=None):
    v_str = ""
    if ge_version:
        v_str += f">={ge_version}"
    if lt_version:
        v_str += f"<{lt_version}"
    if ge_version or lt_version:
        v_str = f" ({v_str})"
    print(f"Checking for Python package '{module_name}'{v_str}...", end="")
    if (module_name, ge_version, lt_version) in requirement_cache:
        print("Cached as found.")
        return
    try:
        try:
            mod = __import__(module_name)
        except ImportError as e:
            raise RequirementError(f"'{module_name}' must have version {v_str}, but is not available.") from e
        if ge_version or lt_version:
            import packaging.version

            if hasattr(mod, "__version__"):
                installed_version = packaging.version.parse(mod.__version__)
            else:
                raise RequirementError(
                    f"'{module_name}' must have version >={ge_version}<{lt_version},"
                    " but has no __version__ attribute."
                )
            requested_min_version = ge_version and packaging.version.parse(ge_version)
            requested_max_version = lt_version and packaging.version.parse(lt_version)
            if (requested_min_version and requested_min_version > installed_version) or (
                requested_max_version and installed_version >= requested_max_version
            ):
                raise RequirementError(
                    f"'{module_name}' must have version >={ge_version}<{lt_version},"
                    f" but have version {installed_version}."
                )
    except RequirementError as e:
        print(str(e))
        raise
    else:
        v = getattr(mod, "__version__", "")
        if v:
            v = " " + v
        print(f"Found{v}.")
        requirement_cache.add(module_name, ge_version, lt_version)


def _get_build_extension():
    from distutils.command.build_ext import build_ext
    from distutils.core import Distribution

    # Modified from Cython/Build/Inline.py, Apache License Version 2.0
    dist = Distribution()
    # Ensure the build respects distutils configuration by parsing
    # the configuration files
    config_files = dist.find_config_files()
    dist.parse_config_files(config_files)
    build_extension = build_ext(dist)
    # build_extension.verbose = True
    build_extension.finalize_options()
    return build_extension


def check_cython_module(name, cython_code, python_code="", extension_options=None):
    extension_options = extension_options or {}
    require_python_module("Cython")
    import Cython.Build.Inline

    print(f"Checking for native {name}...", end="")
    if (cython_code, python_code, extension_options) in requirement_cache:
        print("Cached as found.")
        return
    try:
        module_name = f"_check_cython_module_{abs(hash(cython_code))}"
        with tempfile.TemporaryDirectory() as tmpdir:
            tmpdir = Path(tmpdir)
            pyx_file = tmpdir / f"{module_name}.pyx"
            py_file = tmpdir / f"{module_name}_test.py"
            # Modified from Cython/Build/Inline.py, Apache License Version 2.0
            with open(pyx_file, "w") as fh:
                fh.write(cython_code)
            with open(py_file, "w") as fh:
                fh.write(python_code)
            extension = setuptools.Extension(name=module_name, sources=[str(pyx_file)], **extension_options)
            build_extension = _get_build_extension()
            build_extension.extensions = Cython.Build.cythonize(
                [extension], language_level="3", compiler_directives={"binding": True}, quiet=True
            )
            build_extension.build_temp = os.path.dirname(pyx_file)
            build_extension.build_lib = tmpdir
            build_extension.run()

            # module_path = tmpdir / build_extension.get_ext_filename(module_name)
            # module = Cython.Build.Inline.load_dynamic(module_name, str(module_path))
            subprocess.check_call([sys.executable, str(py_file)], cwd=tmpdir, env={"PYTHONPATH": str(tmpdir)})
    except Exception as e:
        print("Failed.")
        raise RequirementError(f"Could not find native {name}.") from e
    else:
        print("Success.")
        requirement_cache.add(cython_code, python_code, extension_options)


def parse_text(fi):
    result = {}
    for line in fi:
        line = line.rstrip("\n")
        if not line.strip():
            continue
        try:
            key, value = line.split("=", maxsplit=1)
        except ValueError as e:
            raise ValueError("invalid language config file") from e
        result[key] = value
    return result


def load_lang_config(lang):
    """
    Load the compilation configuration provided by CMake.

    PythonSetupSubdirectory.cmake generates a text file that contains build and link flags that we read.
    They typically look like (more text elided with `[...]`, newlines inserted for clarity):
    ::

        COMPILER=ccache;[conda environment]/bin/clang++
        INCLUDE_DIRECTORIES=[src dir]/libquery/include;[...]
        COMPILE_DEFINITIONS=JSON_USE_IMPLICIT_CONVERSIONS=1;[...]
        LINK_OPTIONS=-march=sandybridge;-mtune=generic;
            LINKER:-rpath=[build dir]/external/katana/libgalois;
            LINKER:-rpath=[build dir];
            LINKER:-rpath=/usr/local/katana/lib;[build dir]/external/katana/libgraph/libkatana_graph.so;[...]
        COMPILE_OPTIONS=-g;-Wall;-Wextra;-Wno-deprecated-copy;[...]
        LINKER_WRAPPER_FLAG=-Xlinker,
        LINKER_WRAPPER_FLAG_SEP=,

    This is mostly semi-colon separated lists of command line parameters. However, CMake (3.17+) can generate arguments
    with LINKER: and/or SHELL: prefixes. CMake internally desugars these with CMAKE_<lang>_LINKER_WRAPPER_FLAG and
    CMAKE_<lang>_LINKER_WRAPPER_FLAG_SEP. We reimplement this in `process_linker_option`.
    """
    filename = os.environ.get(f"KATANA_{lang}_CONFIG")
    if not filename:
        return dict(compiler=[], linker=[], extra_compile_args=[], extra_link_args=[], include_dirs=[])

    with open(filename, "rt") as fi:
        config = parse_text(fi)
    linker_wrapper_flag = split_cmake_list(config.get("LINKER_WRAPPER_FLAG", "-Wl,"))
    linker_wrapper_flag_sep = config.get("LINKER_WRAPPER_FLAG_SEP", ",")

    def process_shell_option(opt: str):
        SHELL = "SHELL:"
        if opt.startswith(SHELL):
            opt = opt[len(SHELL) :]
            return opt.split()
        return [opt]

    def process_linker_option(opt: str):
        """
        This is implemented based on: https://cmake.org/cmake/help/latest/command/target_link_options.html
        :param opt: a linker option
        :return: a list of strings that should be used as the real linker options.
        """
        LINKER = "LINKER:"
        if opt.startswith(LINKER):
            opt = opt[len(LINKER) :]
            if linker_wrapper_flag_sep:
                args = [linker_wrapper_flag_sep.join(process_shell_option(opt))]
            else:
                args = process_shell_option(opt)
            if not linker_wrapper_flag:
                return args
            if linker_wrapper_flag[-1] == " ":
                return [b for a in args for b in linker_wrapper_flag[:-1] + [a]]
            return [b for a in args for b in linker_wrapper_flag[:-1] + [linker_wrapper_flag[-1] + a]]
        return process_shell_option(opt)

    return dict(
        compiler=[a for opt in split_cmake_list(config.get("COMPILER")) for a in process_linker_option(opt)],
        extra_compile_args=[f"-D{p}" for p in unique_list(split_cmake_list(config.get("COMPILE_DEFINITIONS")))]
        + [a for opt in split_cmake_list(config.get("COMPILE_OPTIONS")) for a in process_linker_option(opt)],
        extra_link_args=[a for opt in split_cmake_list(config.get("LINK_OPTIONS")) for a in process_linker_option(opt)],
        include_dirs=unique_list(split_cmake_list(config.get("INCLUDE_DIRECTORIES"))),
    )


def find_files(root, prefix, suffix):
    """
    Find files ending with a given suffix in root and its subdirectories and
    return their names relative to root.
    """
    files = []
    for dirpath, _, filenames in os.walk(root):
        for f in filenames:
            if not f.endswith(suffix):
                continue
            relpath = os.path.relpath(dirpath, prefix)
            files.append(Path(relpath) / f)
    return files


def module_name_from_source_name(f, source_root_name):
    parts = []
    for p in f.parents:
        if str(p.name) == source_root_name:
            break
        parts.insert(0, p.name)
    parts.append(f.stem)
    module_name = ".".join(parts)
    return module_name


def process_jinja_file(filename, build_source_root):
    output_file = build_source_root / filename.parent / filename.stem
    output_file.parent.mkdir(parents=True, exist_ok=True)
    for p in filename.parents:
        init_file = build_source_root / p / "__init__.pxd"
        init_file.touch(exist_ok=True)

    # TODO(amp): Ideally this would only process the jinja file when the inputs change. But it's fast and I'm lazy.
    #  https://jinja.palletsprojects.com/en/2.11.x/api/#jinja2.meta.find_referenced_templates
    regenerated = generate_from_jinja.run("python", filename, output_file)
    if regenerated:
        print(f"Processed {filename} with Jinja2.")
    return output_file


def collect_cython_files(source_root):
    """
    Search `source_root` for pyx and pxd files and jinja files which generate them
    and build lists of the existing and generated pyx and pxd files.

    :param source_root: The python source root to search.
    :return: pxd_files, pyx_files
    """
    source_root = Path(source_root)
    pyx_jinja_files = find_files(source_root, source_root.parent, ".pyx.jinja")
    pxd_jinja_files = find_files(source_root, source_root.parent, ".pxd.jinja")

    pxd_files = find_files(source_root, "", ".pxd")
    pyx_files = find_files(source_root, "", ".pyx")

    if not in_build_call():
        print("WARNING: This is not a build call so we are not generating Cython files.", file=sys.stderr)
        return [], []

    for f in pyx_jinja_files + pxd_jinja_files:
        output_file: Path = process_jinja_file(f, source_root.parent)
        if output_file.suffix == ".pyx":
            pyx_files.append(output_file)
        else:
            pxd_files.append(output_file)

    assert all("pyx" in f.name for f in pyx_files)
    assert all("pxd" in f.name for f in pxd_files)

    return pxd_files, pyx_files


def _build_cython_extensions(pyx_files, source_root_name, extension_options):
    cython_extensions = []
    for f in pyx_files:
        module_name = module_name_from_source_name(f, source_root_name)
        cython_extensions.append(setuptools.Extension(module_name, [str(f)], language="c++", **extension_options))

    return cython_extensions


def cythonize(module_list, *, source_root, **kwargs):
    # TODO(amp): Dependencies are yet again repeated here. This needs to come from a central deps list.
    require_python_module("packaging")
    require_python_module("numpy", "1.10")
    try:
        require_python_module("Cython", "0.29.12")
        require_python_module("pyarrow", "6.0")
        build_extensions = True
    except RequirementError:
        print(
            "WARNING: Building Katana Python without extensions! The following features will not work: katana.local, "
            "katana.distributed, katana parallel loops. The following features will work: katana.client and "
            "katana.remote (with limitations).",
            file=sys.stderr,
        )
        build_extensions = False

    if not build_extensions:
        return []

    import Cython.Build
    import numpy
    import pyarrow

    extension_options = load_lang_config("CXX")
    extension_options["include_dirs"].append(numpy.get_include())
    extension_options["include_dirs"].append(pyarrow.get_include())

    if not extension_options["extra_compile_args"]:
        extension_options["extra_compile_args"] = ["-std=c++17", "-Werror"]

    if extension_options["compiler"]:
        compiler = " ".join(extension_options["compiler"])
        os.environ["CXX"] = compiler
        os.environ["CC"] = compiler

        # Because of odd handling of the linker in setuptools with C++ the
        # compiler and the linker must use the same programs, so build a linker
        # command line using the compiler.
        linker = " ".join(extension_options["compiler"] + ["-pthread", "-shared"])
        os.environ["LDSHARED"] = linker
        os.environ["LDEXE"] = linker

    extension_options["extra_compile_args"].extend(
        [
            # Warnings are common in generated code and hard to fix. Don't make them errors.
            "-Wno-error",
            # Entirely disable some warning that are common in generated code and safe.
            "-Wno-unused-variable",
            "-Wno-unused-function",
            "-Wno-deprecated-declarations",
            # Disable numpy deprecation warning in generated code.
            "-DNPY_NO_DEPRECATED_API=NPY_1_7_API_VERSION",
        ]
    )
    extension_options.pop("compiler")

    test_extension_options = extension_options.copy()
    test_extension_options.setdefault("extra_link_args", [])
    if not any(s.endswith("/libkatana_graph.so") for s in test_extension_options["extra_link_args"]):
        test_extension_options["extra_link_args"].append("-lkatana_graph")
    if not any(s.endswith("/libkatana_galois.so") for s in test_extension_options["extra_link_args"]):
        test_extension_options["extra_link_args"].append("-lkatana_galois")
    check_cython_module(
        "libkatana_graph",
        """
# distutils: language=c++
from katana.cpp.libgalois.Galois cimport setActiveThreads, SharedMemSys
cdef SharedMemSys _katana_runtime
setActiveThreads(1)
    """,
        extension_options=test_extension_options,
    )

    source_root = Path(source_root)
    source_root_name = source_root.name

    pyx_files = list(filter(lambda v: isinstance(v, Path), module_list))
    modules = list(filter(lambda v: not isinstance(v, Path), module_list))
    modules.extend(_build_cython_extensions(pyx_files, source_root_name, extension_options))

    kwargs.setdefault("include_path", [])
    kwargs["include_path"].append(str(source_root))
    kwargs["include_path"].append(numpy.get_include())

    return Cython.Build.cythonize(
        modules,
        nthreads=int(os.environ.get("CMAKE_BUILD_PARALLEL_LEVEL", "0")),
        language_level="3",
        compiler_directives={"binding": True},
        **kwargs,
    )


def setup_coverage():
    if os.environ.get("COVERAGE_RCFILE"):
        # usercustomize.py will initialize coverage for each Python process (but only if COVERAGE_PROCESS_START is set).
        with open(Path(os.getcwd()) / "python" / "usercustomize.py", "w") as f:
            f.write("import coverage\n")
            f.write("coverage.process_startup()")


def setup(*, source_dir, package_name, additional_requires=None, package_data=None, **kwargs):
    package_data = package_data or {}
    # TODO(amp): Dependencies are yet again repeated here. This needs to come from a central deps list.
    requires = ["pyarrow (<5.0)", "numpy", "numba (==0.53)"]
    if additional_requires:
        requires.extend(additional_requires)

    setup_coverage()

    source_dir = Path(source_dir)

    pxd_files, pyx_files = collect_cython_files(source_root=source_dir / package_name)

    options = dict(
        version=get_katana_version(),
        name=package_name + "_python",
        packages=setuptools.find_packages(str(source_dir), exclude=("test",), include=(package_name + "*",)),
        package_data={"": [str(f) for f in pxd_files]},
        package_dir={"": str(source_dir)},
        # NOTE: Do not use setup_requires. It doesn't work properly for our needs because it doesn't install the
        # packages in the overall build environment. (It installs them in .eggs in the source tree.)
        requires=requires,
        ext_modules=cythonize(pyx_files, source_root=source_dir),
        include_package_data=True,
        zip_safe=False,
    )
    options["package_data"].update(package_data)
    options.update(kwargs)

    setuptools.setup(**options)


def get_katana_version():
    require_python_module("packaging")
    sys.path.append(str((Path(__file__).parent.parent / "scripts").absolute()))
    import katana_version.version

    return str(katana_version.version.get_version())
