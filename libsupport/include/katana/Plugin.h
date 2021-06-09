#ifndef KATANA_LIBSUPPORT_KATANA_PLUGIN_H_
#define KATANA_LIBSUPPORT_KATANA_PLUGIN_H_

#include <string>
#include <vector>

#include <boost/filesystem.hpp>
#include <katana/config.h>

/// @file Plugin.h
///
/// A simple plugin API. It supports loading shared objects from the directory
/// katana/plugins under the installation lib directory. The plugins are allows
/// to register their features in any Katana library in their KatanaPluginInit
/// function, or using global constructors. The plugins return metadata about
/// themselves so we can track which plugins where loaded and check for version
/// issues.
///
/// All available plugins are loaded when Katana is initialized via SharedMemSys
/// or DistMemSys.

namespace katana {

/// Plugin metadata returned from a plugin.
struct PluginMetadata {
  // This struct is defined using only simple C types to allow non-C++ languages
  // to easily produce an instance if needed to implement a non-C++ plugin.

  /// The human readable name.
  const char* name;

  /// The prose description.
  const char* description;

  /// The version of this plugin, ideally in a PEP-440 compatible format.
  /// (https://www.python.org/dev/peps/pep-0440/#version-scheme)
  const char* version;

  /// The author whether company or individual.
  const char* author;

  /// The name of the license under which the plugin is distributed.
  const char* licence;

  /// A finalizer function to be called if the plugin should be deinitialized.
  /// Plugins cannot be reinitialized after being finalized.
  void (*finalize)();
};

/// Plugin information available about a loaded plugin.
struct Plugin {
  const PluginMetadata* metadata;

  /// The path to the shared object from which this plugin was loaded. This may
  /// be useful for debugging.
  std::string so_path;

  /// The dlopen handle of the shared object for this plugin.
  void* so_handle;
};

/// Declare the KatanaPluginInit function for this shared object.
/// @code
/// namespace {
/// const katana::PluginMetadata plugin_info = {
///     .name = "name",
///     ... other fields ...
/// };
/// }
/// KATANA_PLUGIN_INIT() {
///   ... register features provided by this plugin with Katana ...
///   return &plugin_info;
/// }
/// @endcode
#define KATANA_PLUGIN_INIT()                                                   \
  extern "C" KATANA_EXPORT const katana::PluginMetadata* KatanaPluginInit()

/// Get the paths that katana will search for plugins. Any shared objects in
/// these directories will be loaded as plugins.
KATANA_EXPORT std::vector<boost::filesystem::path> GetPluginPath();

/// Search for and load any plugins installed. Idempotent.
KATANA_EXPORT void LoadPlugins();

/// Search for and load any plugins installed. Idempotent.
/// @return a vector of the Plugin structures.
KATANA_EXPORT std::vector<Plugin> LoadOrGetPlugins();

/// Finalize all plugins.
KATANA_EXPORT void FinalizePlugins();

}  // namespace katana

#endif
