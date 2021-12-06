find_package(pybind11 REQUIRED)

pybind11_add_module(libkatanapython MODULE src/EntityTypeManager.cpp src/LoadAll.cpp src/NUMAArray.cpp src/Reductions.cpp)
target_link_libraries(libkatanapython PRIVATE Katana::support Katana::galois Katana::graph)
