include(FetchContent)
include(ExternalProject)

set(AVA_UNPACK_LOCATION ${CMAKE_CURRENT_BINARY_DIR}/ava)
set(AVA_LLVM_LOCATION ${AVA_UNPACK_LOCATION}/llvm)
set(AVA_CAVA_LOCATION ${AVA_UNPACK_LOCATION}/cava)

FetchContent_Declare(ava
  GIT_REPOSITORY "https://github.com/utcs-scea/ava.git"
  GIT_TAG 38ff7bf54c365a4b
  GIT_SUBMODULES_RECURSE ON
  GIT_PROGRESS ON
  SOURCE_DIR ${AVA_UNPACK_LOCATION}
)

FetchContent_GetProperties(ava)
if(NOT ava_POPULATED)
  FetchContent_Populate(ava)
  add_subdirectory(manager)
endif()

ExternalProject_Add(ava_llvm
  BINARY_DIR ${AVA_LLVM_LOCATION}/build
  DOWNLOAD_COMMAND ""
  CONFIGURE_COMMAND ${CMAKE_COMMAND} -DLLVM_ENABLE_PROJECTS=clang -G "Unix Makefiles" ${AVA_UNPACK_LOCATION}/llvm/llvm
  BUILD_COMMAND make -j4
  INSTALL_COMMAND ""
)

find_package(Python3 COMPONENTS Interpreter)
find_package(LLVM 7.0 CONFIG REQUIRED)
find_package(PkgConfig REQUIRED)
pkg_search_module(GLIB2 REQUIRED glib-2.0)
ExternalProject_Add_Step(ava_llvm nwcc
  COMMAND ${AVA_CAVA_LOCATION}/nwcc ${CMAKE_CURRENT_SOURCE_DIR}/specs/cuda_10_1.c -I /usr/local/cuda-10.1/include -I ${AVA_CAVA_LOCATION}/headers ${GLIB2_CFLAGS}
  WORKING_DIRECTORY ${AVA_CAVA_LOCATION}
  ALWAYS ON
  DEPENDEES install
)
ExternalProject_Add_Step(ava_llvm compile
  COMMAND make clean && make -j2 R=1
  WORKING_DIRECTORY ${AVA_CAVA_LOCATION}/katana_cuda_nw
  DEPENDEES nwcc
)
ExternalProject_Add_Step(ava_llvm link
  COMMAND mkdir -p generated && ln -f -s ${AVA_CAVA_LOCATION}/katana_cuda_nw/libguestlib.so generated/libcudart.so.10.1 && ln -f -s ${AVA_CAVA_LOCATION}/katana_cuda_nw/worker generated/worker
  DEPENDEES compile
)
