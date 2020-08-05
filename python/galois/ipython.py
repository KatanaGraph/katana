from IPython.core.magic import Magics, magics_class, cell_magic

import numpy


@magics_class
class GaloisMagics(Magics):
    @cell_magic
    def galois_cython(self, line, cell):
        "Galois Cython code magic"
        cython_magic = self.shell.find_cell_magic("cython")
        # TODO: This needs to parse the options and add ours to the existing ones if needed.
        header = """
# distutils: extra_link_args=["-lgalois_shmem"]
# distutils: extra_compile_args=["-I{}"]
""".format(
            numpy.get_include()
        )
        cell = header + cell
        return cython_magic(line, cell)
