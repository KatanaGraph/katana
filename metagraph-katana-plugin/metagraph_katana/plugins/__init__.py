############################
# Libraries used as plugins
############################

try:
    import katana as _

    has_katana = True
except ImportError:
    has_katana = False


import metagraph

# Use this as the entry_point object
registry = metagraph.PluginRegistry("katana_for_mg")


def find_plugins():
    # Ensure we import all items we want registered
    from . import katana_for_mg

    registry.register_from_modules(katana_for_mg)
    return registry.plugins
