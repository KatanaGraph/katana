add_library(katana_sanitize SHARED)
add_library(Katana::sanitize ALIAS katana_sanitize)
set_target_properties(katana_sanitize PROPERTIES EXPORT_NAME sanitize)
add_dependencies(lib katana_sanitize)

target_sources(katana_sanitize PRIVATE
  src/dlclose.c
)

set_common_katana_library_options(katana_sanitize ALWAYS_SHARED)
