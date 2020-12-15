# Avoid collisions with existing application targets with the same name.
# The expected public names (without _) are provided using pure python modules.
add_cython_target(_wrappers _wrappers.pyx CXX OUTPUT_VAR WRAPPERS_SOURCES)
add_library(_wrappers MODULE ${WRAPPERS_SOURCES})
python_extension_module(_wrappers)
add_dependencies(_wrappers graphs)
target_link_libraries(_wrappers Galois::shmem)

install(
  TARGETS _wrappers
  LIBRARY DESTINATION python/galois/analytics
)
