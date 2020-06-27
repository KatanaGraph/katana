get_cmake_property(vars CACHE_VARIABLES)
foreach(var ${vars})
  if (var MATCHES ".*_DIR$" OR var MATCHES ".*_ROOT$")
    #message(STATUS "${var} = [${${var}}]")
    list(APPEND CL_ARGS "-D${var}=${${var}}")
  endif()
endforeach()

project(ava-spec)

include(ExternalProject)

###### Build customized LLVM ######

ExternalProject_Add(cava
  PREFIX ava-llvm
  SOURCE_DIR ${CMAKE_CURRENT_SOURCE_DIR}/../llvm/build
  URL "https://github.com/utcs-scea/ava-llvm/releases/download/v7.1.0/ava-llvm-release-7.1.0.tar.gz"
  URL_HASH MD5=400832522ed255314d6a5848e8f7af6c
  BUILD_COMMAND ""
  CONFIGURE_COMMAND ""
  INSTALL_COMMAND ""
  UPDATE_DISCONNECTED ON
)

###### Example: compile cuda_10.1.c ######

find_package(PkgConfig REQUIRED)
pkg_search_module(GLIB2 REQUIRED glib-2.0)
ExternalProject_Add_Step(cava katana-nwcc
  DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/../../specs/cuda_10_1.c
  COMMAND ${CMAKE_CURRENT_SOURCE_DIR}/nwcc ${CMAKE_CURRENT_SOURCE_DIR}/../../specs/cuda_10_1.c -I /usr/local/cuda-10.1/include -I ${CMAKE_CURRENT_SOURCE_DIR}/headers ${GLIB2_CFLAGS}
  WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
  DEPENDEES download
)

ExternalProject_Add_Step(cava katana-link
  COMMAND mkdir -p generated &&
          ln -f -s ${CMAKE_CURRENT_BINARY_DIR}/katana_cuda_nw/libguestlib.so ${CMAKE_CURRENT_BINARY_DIR}/katana_cuda_nw/libcudart.so.10.1
  WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/../..
  DEPENDEES katana-nwcc
)

ExternalProject_Add(katana_cuda_nw
  PREFIX katana_cuda_nw
  SOURCE_DIR ${CMAKE_CURRENT_SOURCE_DIR}/katana_cuda_nw
  BINARY_DIR ${CMAKE_CURRENT_BINARY_DIR}/katana_cuda_nw
  DOWNLOAD_COMMAND ""
  INSTALL_COMMAND ""
  CMAKE_ARGS ${CL_ARGS}
             -DCMAKE_C_COMPILER:STRING=${CMAKE_C_COMPILER}
             -DCMAKE_CXX_COMPILER:STRING=${CMAKE_CXX_COMPILER}
  BUILD_ALWAYS ON
  DEPENDS cava
)
