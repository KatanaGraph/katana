include(ExternalProject)

set(AVA_UNPACK_LOCATION ${CMAKE_BINARY_DIR}/libava/ava)

ExternalProject_Add(ava
  GIT_REPOSITORY https://github.com/utcs-scea/ava.git
  GIT_TAG 38ff7bf54c365a4b
  GIT_SUBMODULES_RECURSE ON
  GIT_PROGRESS ON
  SOURCE_DIR ${AVA_UNPACK_LOCATION}
  CONFIGURE_COMMAND ""
  BUILD_COMMAND ""
  INSTALL_COMMAND ""
 )

add_subdirectory(manager)
