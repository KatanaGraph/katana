include(FetchContent)
include(ExternalProject)

set(AVA_UNPACK_LOCATION ${CMAKE_CURRENT_BINARY_DIR}/ava)
set(AVA_LLVM_LOCATION ${AVA_UNPACK_LOCATION}/llvm)
set(AVA_CAVA_LOCATION ${AVA_UNPACK_LOCATION}/cava)

FetchContent_Declare(ava
  GIT_REPOSITORY "https://github.com/utcs-scea/ava.git"
  GIT_TAG 71d2fc1fd63904ba
  GIT_PROGRESS ON
  SOURCE_DIR ${AVA_UNPACK_LOCATION}
)

FetchContent_GetProperties(ava)
if(NOT ava_POPULATED)
  FetchContent_Populate(ava)
  file(COPY ${CMAKE_CURRENT_SOURCE_DIR}/specs/cava_patch/CMakeLists.txt DESTINATION ${AVA_UNPACK_LOCATION}/cava/)
  file(MAKE_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/specs)
  file(CREATE_LINK ${CMAKE_CURRENT_SOURCE_DIR}/specs/cuda_10_1.c ${CMAKE_CURRENT_BINARY_DIR}/specs/cuda_10_1.c SYMBOLIC)
  file(MAKE_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/ava-bin/generated)
  file(CREATE_LINK ${AVA_CAVA_LOCATION}/headers ${CMAKE_CURRENT_BINARY_DIR}/ava-bin/generated/headers SYMBOLIC)
  add_subdirectory(manager/superbuild)
endif()

ExternalProject_Add(ava-bin
  BINARY_DIR ${CMAKE_CURRENT_BINARY_DIR}/ava-bin
  DOWNLOAD_COMMAND ""
  CONFIGURE_COMMAND ${CMAKE_COMMAND} ${AVA_UNPACK_LOCATION}
  BUILD_COMMAND make -j4
  INSTALL_COMMAND ""
)

ExternalProject_Add_Step(ava-bin link
  COMMAND mkdir -p generated &&
          ln -f -s ${CMAKE_CURRENT_BINARY_DIR}/ava-bin/generated/katana_cuda_nw/libguestlib.so generated/libcudart.so.10.1 &&
          ln -f -s ${CMAKE_CURRENT_BINARY_DIR}/ava-bin/generated/katana_cuda_nw/worker generated/worker
  DEPENDEES install
)
