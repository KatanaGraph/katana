from setuptools import setup, find_packages
import versioneer

setup(
    name="metagraph-katana",
    version=versioneer.get_version(),
    cmdclass=versioneer.get_cmdclass(),
    description="katana plugins for Metagraph",
    author="Anaconda, Inc.",
    packages=find_packages(include=["metagraph_katana", "metagraph_katana.*"]),
    include_package_data=True,
    install_requires=["metagraph", "katana", "icecream"],
    entry_points={
        "metagraph.plugins": "plugins=metagraph_katana.plugins:find_plugins"
    },
)
