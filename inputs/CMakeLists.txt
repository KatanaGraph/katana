set(INPUT_VERSION v10)

set(INPUT_URL "https://katana-ci-public.s3.us-east-1.amazonaws.com/inputs/katana-inputs-${INPUT_VERSION}.tar.gz")

file(
  WRITE ${CMAKE_CURRENT_BINARY_DIR}/download.cmake
  "file(DOWNLOAD ${INPUT_URL} ${CMAKE_CURRENT_BINARY_DIR}/inputs-${INPUT_VERSION}.tar.gz SHOW_PROGRESS)")

add_custom_command(
  OUTPUT inputs-${INPUT_VERSION}.tar.gz
  COMMAND ${CMAKE_COMMAND} -P ${CMAKE_CURRENT_BINARY_DIR}/download.cmake
  COMMAND ${CMAKE_COMMAND} -E make_directory ${CMAKE_CURRENT_BINARY_DIR}/${INPUT_VERSION}
  COMMENT "Downloading inputs"
  VERBATIM
)

add_custom_command(
  OUTPUT ${INPUT_VERSION}/extracted
  DEPENDS inputs-${INPUT_VERSION}.tar.gz
  COMMAND ${CMAKE_COMMAND} -E tar xJf ${CMAKE_CURRENT_BINARY_DIR}/inputs-${INPUT_VERSION}.tar.gz
  COMMAND ${CMAKE_COMMAND} -E touch extracted
  WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/${INPUT_VERSION}
  COMMENT "Unpacking inputs"
  VERBATIM
)

add_custom_command(
  OUTPUT current/current-${INPUT_VERSION}
  DEPENDS ${INPUT_VERSION}/extracted
  COMMAND ${CMAKE_COMMAND} -E create_symlink ${INPUT_VERSION} current
  COMMAND ${CMAKE_COMMAND} -E touch ${INPUT_VERSION}/current-${INPUT_VERSION}
  COMMENT "Updating inputs"
  VERBATIM
)

add_custom_target(input ALL DEPENDS current/current-${INPUT_VERSION})
