project(ava-manager C CXX CUDA)

find_package(Threads REQUIRED)
find_package(PkgConfig REQUIRED)

set(protobuf_MODULE_COMPATIBLE TRUE)
find_package(Protobuf CONFIG REQUIRED)
message(STATUS "Using protobuf ${Protobuf_VERSION}")
set(_PROTOBUF_LIBPROTOBUF protobuf::libprotobuf)

find_package(Flatbuffers CONFIG REQUIRED)
message(STATUS "Using Flatbuffers ${Flatbuffers_VERSION}")
if(CMAKE_CROSSCOMPILING)
  find_program(_FLATBUFFERS_FLATC flatc)
else()
  set(_FLATBUFFERS_FLATC $<TARGET_FILE:flatbuffers::flatc>)
endif()
set(_FLATBUFFERS flatbuffers::flatbuffers)

find_package(gRPC CONFIG REQUIRED)
message(STATUS "Using gRPC ${gRPC_VERSION}")
if(CMAKE_CROSSCOMPILING)
  find_program(_GRPC_CPP_PLUGIN_EXECUTABLE grpc_cpp_plugin)
else()
  set(_GRPC_CPP_PLUGIN_EXECUTABLE $<TARGET_FILE:gRPC::grpc_cpp_plugin>)
endif()
set(_REFLECTION gRPC::grpc++_reflection)
set(_GRPC_GRPCPP gRPC::grpc++)

# Proto file
get_filename_component(manager_service_proto "manager_service.fbs" ABSOLUTE)
get_filename_component(manager_service_proto_path "${manager_service_proto}" PATH)

# Generated sources
set(manager_service_proto_hdrs "${CMAKE_CURRENT_BINARY_DIR}/manager_service_generated.h")
set(manager_service_grpc_srcs  "${CMAKE_CURRENT_BINARY_DIR}/manager_service.grpc.fb.cc")
set(manager_service_grpc_hdrs  "${CMAKE_CURRENT_BINARY_DIR}/manager_service.grpc.fb.h")
add_custom_command(
  OUTPUT "${manager_service_proto_hdrs}"
         "${manager_service_grpc_srcs}"
         "${manager_service_grpc_hdrs}"
  COMMAND ${_FLATBUFFERS_FLATC}
  ARGS --cpp --grpc
       -o "${CMAKE_CURRENT_BINARY_DIR}"
       -I "${manager_service_proto_path}"
       "${manager_service_proto}"
  DEPENDS "${manager_service_proto}"
)

include_directories("${CMAKE_CURRENT_BINARY_DIR}")
include_directories(${CMAKE_CUDA_TOOLKIT_INCLUDE_DIRECTORIES})

set(AVA_UNPACK_LOCATION ${CMAKE_CURRENT_BINARY_DIR}/../ava)
find_library(NVML_LIB nvidia-ml ${CMAKE_CUDA_IMPLICIT_LINK_DIRECTORIES})
add_compile_options(-Wall -Wextra -pedantic -Wno-write-strings)
include_directories(${AVA_UNPACK_LOCATION}/include)
add_executable(manager
  manager.cpp
  ${AVA_UNPACK_LOCATION}/common/socket.cpp
  ${manager_service_grpc_srcs}
)
target_link_libraries(manager
  ${_REFLECTION}
  ${_GRPC_GRPCPP}
  ${_PROTOBUF_LIBPROTOBUF}
  ${_FLATBUFFERS}
  Threads::Threads
  ${NVML_LIB}
)

add_executable(spawn_daemon
  spawn_daemon.cpp
  ${manager_service_grpc_srcs}
)
target_link_libraries(spawn_daemon
  ${_REFLECTION}
  ${_GRPC_GRPCPP}
  ${_PROTOBUF_LIBPROTOBUF}
  ${_FLATBUFFERS}
  ${NVML_LIB}
)
