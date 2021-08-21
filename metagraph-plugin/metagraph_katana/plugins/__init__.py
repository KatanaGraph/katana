############################
# Libraries used as plugins
############################

# try:
#     import katana as _

#     has_katana = True
# except ImportError:
#     has_katana = False


import metagraph

# Use this as the entry_point object
registry = metagraph.PluginRegistry("metagraph_katana")


def find_plugins():
    # Ensure we import all items we want registered
    from . import metagraph_katana

    registry.register_from_modules(metagraph_katana)
    return registry.plugins
