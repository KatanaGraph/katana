message(WARNING
        "${CMAKE_CURRENT_LIST_FILE} is only processed to assist IDEs. "
        "The libraries it describes are not used.")

find_package(pybind11 REQUIRED)

pybind11_add_module(libkatanapython MODULE EntityTypeManager.cpp LoadAll.cpp NUMAArray.cpp Reductions.cpp)
target_link_libraries(libkatanapython PRIVATE Katana::support Katana::galois Katana::graph)
