import sys
import os
import warnings
from pathlib import Path
import numpy
import setuptools
import Cython.Build
import configparser
from packaging.version import Version

try:
    from sphinx.setup_command import BuildDoc
except ImportError:
    BuildDoc = None

import generate_from_jinja

__all__ = ["setup"]


def split_cmake_list(s):
    return list(filter(None, s.split(";")))


def unique_list(l):
    return list(dict.fromkeys(l))


def load_lang_config(lang):
    filename = os.environ.get(f"KATANA_{lang}_CONFIG")
    if not filename:
        return dict(compiler=[], linker=[], extra_compile_args=[], extra_link_args=[], include_dirs=[])
    parser = configparser.ConfigParser()
    parser.read(filename, encoding="UTF-8")
    return dict(
        compiler=split_cmake_list(parser.get("build", "COMPILER")),
        extra_compile_args=[f"-D{p}" for p in unique_list(split_cmake_list(parser.get("build", "COMPILE_DEFINITIONS")))]
        + split_cmake_list(parser.get("build", "COMPILE_OPTIONS")),
        extra_link_args=split_cmake_list(parser.get("build", "LINK_OPTIONS")),
        include_dirs=unique_list(split_cmake_list(parser.get("build", "INCLUDE_DIRECTORIES"))),
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
    regenerated = generate_from_jinja.run("python" / filename.parent, filename.name, output_file)
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
    extension_options = load_lang_config("CXX")
    extension_options["include_dirs"].append(numpy.get_include())

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

    is_clang = any("clang" in s for s in extension_options["compiler"])

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

    source_root = Path(source_root)
    source_root_name = source_root.name

    pyx_files = list(filter(lambda v: isinstance(v, Path), module_list))
    modules = list(filter(lambda v: not isinstance(v, Path), module_list))
    modules.extend(_build_cython_extensions(pyx_files, source_root_name, extension_options))

    kwargs.setdefault("include_path", [])
    kwargs["include_path"].append(str(source_root))
    kwargs["include_path"].append(numpy.get_include())

    # with warnings.catch_warnings():
    # warnings.filterwarnings("ignore", message="build_dir has no effect for absolute source paths")
    return Cython.Build.cythonize(
        modules, nthreads=int(os.environ.get("CMAKE_BUILD_PARALLEL_LEVEL", "0")), language_level="3", **kwargs
    )


def setup(*, source_dir, package_name, doc_package_name, **kwargs):
    # Require pytest-runner only when running tests
    pytest_runner = ["pytest-runner>=2.0,<3dev"] if any(arg in sys.argv for arg in ("pytest", "test")) else []

    setup_requires = ["numpy"] + pytest_runner

    source_dir = Path(source_dir).absolute()

    pxd_files, pyx_files = collect_cython_files(source_root=source_dir / package_name)

    options = dict(
        version=get_katana_version(),
        name=package_name + "_python",
        packages=setuptools.find_packages(str(source_dir), exclude=("tests",)),
        package_data={"": [str(f) for f in pxd_files]},
        package_dir={"": str(source_dir)},
        tests_require=["pytest"],
        setup_requires=setup_requires,
        ext_modules=cythonize(pyx_files, source_root=source_dir),
        zip_safe=False,
        command_options={
            "build_sphinx": {
                "project": ("setup.py", doc_package_name),
                "version": ("setup.py", get_katana_version()),
                "release": ("setup.py", get_katana_version()),
                "copyright": ("setup.py", get_katana_copyright_year()),
                "source_dir": ("setup.py", str(source_dir / "docs")),
            }
        },
    )
    if BuildDoc:
        options.update(cmdclass={"build_sphinx": BuildDoc})
    options.update(kwargs)

    setuptools.setup(**options)


def get_katana_version():
    sys.path.append(str((Path(__file__).parent.parent / "scripts").absolute()))
    import katana_version.version

    return str(katana_version.version.get_version())


def get_katana_copyright_year():
    year = os.environ.get("KATANA_COPYRIGHT_YEAR")
    if not year:
        year = "2021"
    return year
