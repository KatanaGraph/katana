generate_cython_source(_pyarrow_wrappers DEPENDS)
add_cython_target(_pyarrow_wrappers _pyarrow_wrappers.pyx CXX OUTPUT_VAR PYARROW_WRAPPERS_SOURCES)
add_library(_pyarrow_wrappers MODULE ${PYARROW_WRAPPERS_SOURCES})
python_extension_module(_pyarrow_wrappers)
# target_link_libraries(_pyarrow_wrappers NumPy)

add_cython_target(_native_wrapper_utils _native_wrapper_utils.pyx CXX OUTPUT_VAR NATIVE_WRAPPER_UTILS_SOURCES)
add_library(_native_wrapper_utils MODULE ${NATIVE_WRAPPER_UTILS_SOURCES})
python_extension_module(_native_wrapper_utils)

install(
  TARGETS _pyarrow_wrappers _native_wrapper_utils
  LIBRARY DESTINATION python/galois/numba
)
