def load_ipython_extension(ipython):
    import cython
    cython.load_ipython_extension(ipython)
    from .ipython import GaloisMagics
    ipython.register_magics(GaloisMagics)
