set(INPUT_VERSION v14)

set(INPUT_URL "https://katana-ci-public.s3.us-east-1.amazonaws.com/inputs/katana-inputs-${INPUT_VERSION}.tar.gz")

# Use CMAKE_BINARY_DIR here because it makes sure the inputs are always in the top-level binary directory regardless
# of how projects are combined.

set(INPUTS_DIR ${CMAKE_BINARY_DIR}/inputs)

file(
  WRITE ${INPUTS_DIR}/download.cmake
  "file(DOWNLOAD ${INPUT_URL} ${INPUTS_DIR}/inputs-${INPUT_VERSION}.tar.gz SHOW_PROGRESS)")

add_custom_command(
  OUTPUT inputs-${INPUT_VERSION}.tar.gz
  COMMAND ${CMAKE_COMMAND} -P ${INPUTS_DIR}/download.cmake
  COMMAND ${CMAKE_COMMAND} -E make_directory ${INPUTS_DIR}/${INPUT_VERSION}
  COMMENT "Downloading inputs"
  VERBATIM
)

add_custom_command(
  OUTPUT ${INPUT_VERSION}/extracted
  DEPENDS inputs-${INPUT_VERSION}.tar.gz
  COMMAND ${CMAKE_COMMAND} -E tar xJf ${INPUTS_DIR}/inputs-${INPUT_VERSION}.tar.gz
  COMMAND ${CMAKE_COMMAND} -E touch extracted
  WORKING_DIRECTORY ${INPUTS_DIR}/${INPUT_VERSION}
  COMMENT "Unpacking inputs"
  VERBATIM
)

add_custom_command(
  OUTPUT current/current-${INPUT_VERSION}
  DEPENDS ${INPUT_VERSION}/extracted
  COMMAND ${CMAKE_COMMAND} -E create_symlink ${INPUT_VERSION} current
  COMMAND ${CMAKE_COMMAND} -E touch ${INPUT_VERSION}/current-${INPUT_VERSION}
  WORKING_DIRECTORY ${INPUTS_DIR}
  COMMENT "Updating inputs"
  VERBATIM
)

add_custom_target(input ALL DEPENDS current/current-${INPUT_VERSION})
