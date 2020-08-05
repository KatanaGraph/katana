import pathlib
import os
import galois


def install_in_environ():
    """
    Modify the process environment variables to allow Python and Cython to find
    the Galois libraries and headers.
    """

    def add_path_to_environ_var(var_name, rel_path, base_path):
        new_path = (base_path / rel_path).resolve()
        if os.environ.get("DEBUG"):
            print("Maybe adding {} to {} ({})".format(new_path, var_name, new_path.exists()))
        if not new_path.exists():
            return
        if var_name in os.environ:
            old_list = list(filter(None, os.environ[var_name].split(os.pathsep)))
        else:
            old_list = []
        new_list = old_list + [str(new_path)]
        os.environ[var_name] = os.pathsep.join(new_list)

    bases = [
        pathlib.Path(galois.__file__).parent.parent.parent.parent / "cmake-install",  # In build directory
        pathlib.Path(galois.__file__).parent.parent,  # Installed with setup.py install
        pathlib.Path(os.environ["HOME"]) / ".local",  # Installed from wheel
    ]
    for base in bases:
        add_path_to_environ_var("LIBRARY_PATH", "lib", base)
        add_path_to_environ_var("LD_LIBRARY_PATH", "lib", base)
        add_path_to_environ_var("CPLUS_INCLUDE_PATH", "include", base)


if __name__ == "__main__":
    import sys
    import subprocess

    # Setup the environment and then run the command line as a new command in the modified environ.
    install_in_environ()
    r = subprocess.run(sys.argv[1:])
    sys.exit(r.returncode)
