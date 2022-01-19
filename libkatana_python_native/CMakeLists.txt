find_package(pybind11 REQUIRED)

add_library(katana_python_native SHARED)
add_library(Katana::python_native ALIAS katana_python_native)
add_dependencies(lib katana_python_native)

set(sources
    src/Reductions.cpp
    src/EntityTypeManager.cpp
    src/ImportData.cpp
    )

target_sources(katana_python_native PRIVATE ${sources})

target_include_directories(katana_python_native PUBLIC
                           $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
                           $<INSTALL_INTERFACE:include>
                           )

target_link_libraries(katana_python_native PUBLIC katana_galois)
target_link_libraries(katana_python_native PUBLIC katana_graph)
target_link_libraries(katana_python_native PUBLIC katana_support)

set_common_katana_library_options(katana_python_native)

# Pybind11 dependencies to enable the way it wants to be built.
target_link_libraries(katana_python_native PRIVATE pybind11::headers pybind11::python_headers)
if (TARGET pybind11::thin_lto)
  target_link_libraries(katana_python_native PRIVATE pybind11::thin_lto)
endif()


# Do not install headers since we do not support third-party C++ Python integration (at least yet)

install(
    TARGETS katana_python_native
    LIBRARY
    DESTINATION "${CMAKE_INSTALL_LIBDIR}"
    COMPONENT shlib
    ARCHIVE
    DESTINATION "${CMAKE_INSTALL_LIBDIR}"
    COMPONENT lib
    INCLUDES DESTINATION "${CMAKE_INSTALL_INCLUDEDIR}"
)
