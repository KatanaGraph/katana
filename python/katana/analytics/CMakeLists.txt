# Avoid collisions with existing application targets with the same name.
# The expected public names (without _) are provided using pure python modules.

add_cython_target(plan plan.pyx CXX OUTPUT_VAR PLAN_SOURCES)
add_library(plan MODULE ${PLAN_SOURCES})
python_extension_module(plan)
target_link_libraries(plan Katana::galois)

add_cython_target(_wrappers _wrappers.pyx CXX OUTPUT_VAR WRAPPERS_SOURCES)
add_library(_wrappers MODULE ${WRAPPERS_SOURCES})
python_extension_module(_wrappers)
add_dependencies(_wrappers plan)
target_link_libraries(_wrappers Katana::galois)

install(
  TARGETS _wrappers plan
  LIBRARY DESTINATION python/katana/analytics
)
