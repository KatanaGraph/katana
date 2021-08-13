from setuptools import find_packages, setup

#import katana

setup(
    name="metagraph-katana",
    #version=katana.__version__,
    version='0.0.1',
    # currently the version is the same as katana.__version__ (which actually comes from the version built into libsupport).
    # Eventually we need to have separate versions built into each library
    description="katana plugins for Metagraph",
    author="Katana Graph, Inc.",
    packages=find_packages(include=["metagraph_katana", "metagraph_katana.*"]),
    include_package_data=True,
    install_requires=["metagraph", "katana-python"],
    entry_points={"metagraph.plugins": "plugins=metagraph_katana.plugins:find_plugins"},
)
