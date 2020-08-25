generate_cython_source(_pyarrow_wrappers DEPENDS)
add_cython_target(_pyarrow_wrappers _pyarrow_wrappers.pyx CXX OUTPUT_VAR PYARROW_WRAPPERS_SOURCES)
add_library(_pyarrow_wrappers MODULE ${PYARROW_WRAPPERS_SOURCES})
python_extension_module(_pyarrow_wrappers)
# target_link_libraries(_pyarrow_wrappers NumPy)

install(
  TARGETS _pyarrow_wrappers
  LIBRARY DESTINATION python/galois/numba_support
)
