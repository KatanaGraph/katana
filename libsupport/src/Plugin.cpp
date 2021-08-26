#include "katana/Plugin.h"

#include <dlfcn.h>

#include <cstdlib>
#include <unordered_map>

#include <boost/filesystem.hpp>
#include <katana/Logging.h>
#include <katana/Random.h>
#include <katana/Result.h>
#include <katana/Strings.h>

namespace {

class PluginLoader {
  std::unordered_map<std::string, katana::Plugin> plugins_;

public:
  void LoadPlugin(const boost::filesystem::path& path) {
    if (plugins_.count(path.string())) {
      return;
    }

    void* handle = dlopen(path.c_str(), RTLD_LAZY | RTLD_LOCAL);
    if (!handle) {
      KATANA_LOG_WARN(
          "skipping plugin {}: failed to dynamically link plugin: {}", path,
          katana::ResultErrno());
      return;
    }
    const auto KatanaPluginInit =
        reinterpret_cast<const katana::PluginMetadata* (*)()>(
            dlsym(handle, "KatanaPluginInit"));
    const katana::PluginMetadata* metadata = KatanaPluginInit();
    if (!metadata) {
      KATANA_LOG_WARN("plugin {} may have failed to load", path);
    }
    KATANA_LOG_VERBOSE("loaded plugin: {}", path);
    plugins_.emplace(
        path.string(), katana::Plugin{
                           .metadata = metadata,
                           .so_path = path.string(),
                           .so_handle = handle,
                       });
  }

  void LoadAllPlugins() {
    if (!plugins_.empty()) {
      return;
    }

    // Find all the plugins we will load in the paths.
    auto path = katana::GetPluginPath();
    std::vector<boost::filesystem::path> plugin_paths;
    for (const auto& p : path) {
      for (const boost::filesystem::directory_entry& entry :
           boost::filesystem::directory_iterator(
               p, boost::filesystem::directory_options::none)) {
        // TODO(amp): This only supports Linux.
        if (entry.path().extension() == ".so") {
          boost::system::error_code err{};
          boost::filesystem::path file = canonical(entry.path(), err);
          if (err) {
            KATANA_LOG_WARN(
                "skipping plugin {}: could not canonicalize plugin path: {}",
                entry.path(), err);
          }
          if (is_regular_file(file, err)) {
            plugin_paths.emplace_back(std::move(file));
          } else {
            KATANA_LOG_WARN("skipping plugin {}: not a regular file", file);
          }
          if (err) {
            KATANA_LOG_WARN(
                "skipping plugin {}: could not stat plugin: {}", file, err);
          }
        }
      }
    }

    // Shuffle the list to make sure any assumptions about loading order are
    // invalidated during testing. This will avoid depending on specific load
    // orders.
    std::shuffle(
        plugin_paths.begin(), plugin_paths.end(), katana::GetGenerator());

    // Now load all the plugins in the random order.
    for (const auto& file : plugin_paths) {
      LoadPlugin(file);
    }
  }

  std::vector<katana::Plugin> plugins() const {
    std::vector<katana::Plugin> ret(plugins_.size());

    transform(plugins_.begin(), plugins_.end(), ret.begin(), [&](auto p) {
      return p.second;
    });

    return ret;
  }

  void FinalizePlugins() {
    for (const auto& entry : plugins_) {
      const katana::Plugin& plugin = entry.second;
      plugin.metadata->finalize();
      dlclose(plugin.so_handle);
    }
    plugins_.clear();
  }
};

// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
PluginLoader plugin_loader;

}  // namespace

std::vector<boost::filesystem::path>
katana::GetPluginPath() {
  std::vector<boost::filesystem::path> ret;
  bool found = false;

  // 1) User provided paths from KATANA_PLUGIN_PATH overriding all other paths
  const char* env_var_str = std::getenv("KATANA_PLUGIN_PATH");
  if (env_var_str) {
    std::string env_var_path(env_var_str);
    for (const auto& s : katana::SplitView(env_var_path, ":")) {
      if (!s.empty()) {
        ret.emplace_back(std::string(s));
        found = true;
      }
    }
  } else {
    // 2) Path relative to the shared object
    Dl_info dl_info;
    // Get the path of the shared object containing this function and compute
    // paths relative to it.
    if (dladdr(reinterpret_cast<void*>(&GetPluginPath), &dl_info)) {
      boost::filesystem::path this_so_filename(dl_info.dli_fname);
      boost::system::error_code err{};
      this_so_filename = canonical(this_so_filename, err);
      if (!err) {
        // 2a) installed shared objects
        auto path = canonical(
            this_so_filename.parent_path() / "katana" / "plugins", err);
        if (!err && is_directory(path)) {
          ret.emplace_back(path);
          // Return immediately if we find the installation path so as not to
          // find build look-a-like directories in that case.
          return ret;
        }
        // 2b) open build directory
        path = canonical(
            this_so_filename.parent_path().parent_path() / "plugins", err);
        if (!err && is_directory(path)) {
          ret.emplace_back(path);
          KATANA_WARN_ONCE("using build directory plugins at: {}", path);
          found = true;
        }
        // 2b) enterprise build directory
        path = canonical(
            this_so_filename.parent_path()
                    .parent_path()
                    .parent_path()
                    .parent_path() /
                "plugins",
            err);
        if (!err && is_directory(path)) {
          ret.emplace_back(path);
          KATANA_WARN_ONCE("using build directory plugins at: {}", path);
          found = true;
        }
      }
    }
    if (!found) {
      KATANA_WARN_ONCE(
          "could not determine installation path of libkatana_support. Plugins "
          "installed along side it may not be found. (Work around: set "
          "KATANA_PLUGIN_PATH)");
    }
  }
  // TODO(amp): The above will only work for dynamically linked programs. If we
  //  ever want to support static linking, then we should search relative to the
  //  root executable as well.

  return ret;
}

std::vector<katana::Plugin>
katana::LoadOrGetPlugins() {
  plugin_loader.LoadAllPlugins();
  return plugin_loader.plugins();
}

void
katana::LoadPlugins() {
  plugin_loader.LoadAllPlugins();
}

void
katana::FinalizePlugins() {
  plugin_loader.FinalizePlugins();
}
