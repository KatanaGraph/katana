add_library(katana_disable_dlclose SHARED)
add_library(Katana::disable_dlclose ALIAS katana_disable_dlclose)
set_target_properties(katana_disable_dlclose PROPERTIES EXPORT_NAME disable_dlclose)
add_dependencies(lib katana_disable_dlclose)

target_sources(katana_disable_dlclose PRIVATE
  src/dlclose.c
)

set_common_katana_library_options(katana_disable_dlclose ALWAYS_SHARED)

add_subdirectory(test)
