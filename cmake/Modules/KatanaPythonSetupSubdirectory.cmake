include_guard(DIRECTORY)

function(_symlink_tree TARGET_NAME SOURCE DEST)
  if (NOT IS_ABSOLUTE ${SOURCE})
    set(SOURCE ${CMAKE_CURRENT_SOURCE_DIR}/${SOURCE})
  endif()

  if (NOT IS_ABSOLUTE ${DEST})
    set(SOURCE ${CMAKE_CURRENT_BINARY_DIR}/${DEST})
  endif()

  if (IS_DIRECTORY ${SOURCE})
    get_filename_component(dir_name ${SOURCE} NAME)
    string(APPEND dir_name "/")
    get_filename_component(parent ${SOURCE} DIRECTORY)
    file(GLOB_RECURSE files
         RELATIVE ${parent}
         CONFIGURE_DEPENDS
         LIST_DIRECTORIES false
         ${SOURCE}/*)
  else()
    if(NOT EXISTS ${SOURCE})
      message(SEND_ERROR "Missing file: ${SOURCE}")
    endif()
    get_filename_component(files ${SOURCE} NAME)
    get_filename_component(parent ${SOURCE} DIRECTORY)
  endif()

  file(GENERATE OUTPUT ${TARGET_NAME}.cmake CONTENT [[
    if (IS_DIRECTORY ${SOURCE})
      get_filename_component(dir_name ${SOURCE} NAME)
      string(APPEND dir_name "/")
      get_filename_component(parent ${SOURCE} DIRECTORY)
      file(GLOB_RECURSE files
           RELATIVE ${parent}
           LIST_DIRECTORIES false
           ${SOURCE}/*)
    else()
      if(NOT EXISTS ${SOURCE})
        message(SEND_ERROR "Missing file: ${SOURCE}")
      endif()
      get_filename_component(files ${SOURCE} NAME)
      get_filename_component(parent ${SOURCE} DIRECTORY)
    endif()

    get_filename_component(source_suffix ${SOURCE} NAME)
    set(full_dest "${DEST}/${source_suffix}")
    if (EXISTS ${full_dest})
      file(GLOB_RECURSE symlinks
           LIST_DIRECTORIES false
           ${full_dest}/*)
      foreach(f IN LISTS symlinks full_dest)
        if (IS_SYMLINK ${f})
          file(REMOVE ${f})
        else()
        endif()
      endforeach()
    endif()

    foreach(f IN LISTS files)
      get_filename_component(directory ${DEST}/${f} DIRECTORY)
      file(MAKE_DIRECTORY ${directory})
      file(RELATIVE_PATH rpath ${directory} ${parent}/${f})
      file(CREATE_LINK ${rpath} ${DEST}/${f} SYMBOLIC COPY_ON_ERROR)
    endforeach()
  ]])
  set(files_full "${files}")
  list(TRANSFORM files_full PREPEND "${DEST}/")
  add_custom_target(
      ${TARGET_NAME}
      COMMAND ${CMAKE_COMMAND} -DSOURCE="${SOURCE}" -DDEST="${DEST}" -P ${TARGET_NAME}.cmake
      BYPRODUCTS ${files_full}
      COMMENT "Updating symlinks to ${SOURCE} from ${DEST}"
  )
endfunction()

function(_generate_build_configuration_ini)
  set(no_value_options)
  set(one_value_options FILE_PREFIX)
  set(multi_value_options DEPENDS)

  cmake_parse_arguments(X "${no_value_options}" "${one_value_options}" "${multi_value_options}" ${ARGN})

  get_directory_property(INCLUDE_DIRECTORIES INCLUDE_DIRECTORIES)
  list(APPEND INCLUDE_DIRECTORIES "${INCLUDE_DIRECTORIES}")
  get_directory_property(COMPILE_DEFINITIONS COMPILE_DEFINITIONS)
  list(APPEND COMPILE_DEFINITIONS "${COMPILE_DEFINITIONS}")
  get_directory_property(LINK_OPTIONS LINK_OPTIONS)
  list(APPEND LINK_OPTIONS "${LINK_OPTIONS}")
  get_directory_property(COMPILE_OPTIONS COMPILE_OPTIONS)
  list(APPEND COMPILE_OPTIONS "${COMPILE_OPTIONS}")

  if (CMAKE_CXX_STANDARD)
    list(APPEND COMPILE_OPTIONS "$<$<COMPILE_LANGUAGE:CXX>:-std=c++${CMAKE_CXX_STANDARD}>")
  endif ()

  foreach(TARGET_NAME IN LISTS X_DEPENDS)
    get_target_property(X ${TARGET_NAME} PYTHON_ENV_SCRIPT)
    if (X)
      continue()
    endif()
    list(APPEND INCLUDE_DIRECTORIES "$<TARGET_PROPERTY:${TARGET_NAME},INTERFACE_INCLUDE_DIRECTORIES>")
    list(APPEND COMPILE_DEFINITIONS "$<TARGET_PROPERTY:${TARGET_NAME},INTERFACE_COMPILE_DEFINITIONS>")
    list(APPEND LINK_OPTIONS "$<TARGET_PROPERTY:${TARGET_NAME},INTERFACE_LINK_OPTIONS>")
    list(APPEND LINK_OPTIONS "-Wl,-rpath=$<TARGET_LINKER_FILE_DIR:${TARGET_NAME}>")
    list(APPEND LINK_OPTIONS "-Wl,-rpath=$<JOIN:$<TARGET_PROPERTY:${TARGET_NAME},BUILD_RPATH>,;-Wl,-rpath=>")
    list(APPEND LINK_OPTIONS "-Wl,-rpath=$<JOIN:$<TARGET_PROPERTY:${TARGET_NAME},INSTALL_RPATH>,;-Wl,-rpath=>")
    list(APPEND LINK_OPTIONS "$<TARGET_LINKER_FILE:${TARGET_NAME}>")
    list(APPEND COMPILE_OPTIONS "$<TARGET_PROPERTY:${TARGET_NAME},INTERFACE_COMPILE_OPTIONS>")
  endforeach()

  set(compiler
      "$<$<COMPILE_LANGUAGE:CXX>:${CMAKE_CXX_COMPILER_LAUNCHER};${CMAKE_CXX_COMPILER}>$<$<COMPILE_LANGUAGE:C>:${CMAKE_C_COMPILER_LAUNCHER};${CMAKE_C_COMPILER}>")
  #  $<$<COMPILE_LANGUAGE:CXX>:${CMAKE_CXX_COMPILER}>$<$<COMPILE_LANGUAGE:C>:${CMAKE_C_COMPILER}>
  file(GENERATE OUTPUT ${X_FILE_PREFIX}$<COMPILE_LANGUAGE>.ini
       CONTENT "[build]
COMPILER=${compiler}
INCLUDE_DIRECTORIES=${INCLUDE_DIRECTORIES}
COMPILE_DEFINITIONS=${COMPILE_DEFINITIONS}
LINK_OPTIONS=${LINK_OPTIONS}
COMPILE_OPTIONS=${COMPILE_OPTIONS}
")
endfunction()

function(add_python_setuptools_target TARGET_NAME)
  set(no_value_options)
  set(one_value_options SETUP_DIRECTORY)
  set(multi_value_options DEPENDS)

  cmake_parse_arguments(X "${no_value_options}" "${one_value_options}" "${multi_value_options}" ${ARGN})

  set(PYTHON_SETUP_DIR ${X_SETUP_DIRECTORY})
  if(NOT PYTHON_SETUP_DIR)
    set(PYTHON_SETUP_DIR ${CMAKE_CURRENT_SOURCE_DIR})
  endif()

  set(PYTHON_BINARY_DIR ${CMAKE_CURRENT_BINARY_DIR}/${TARGET_NAME}_build)
  set(PYTHON_ENV_SCRIPT ${CMAKE_CURRENT_BINARY_DIR}/python_env.sh)

  # We build a symlink tree from the build directory to the source and perform the build in that tree. This
  # makes the build in-tree from the perspective of the Python package, but out-of-tree from the perspective of
  # cmake. This odd approach is needed because Python, setuptools, and Cython do not work reliably
  # with out-of-tree builds and a true in-tree build is distasteful and dangerous because we generate files
  # with the same extensions as files that are included in the repository making correctly cleaning build files
  # from an in-tree build very error prone. Past attempts at out-of-tree and true in-tree builds produced build
  # inconsistencies that required manual cleaning to fix.

  _symlink_tree(${TARGET_NAME}_python_tree ${PYTHON_SETUP_DIR}/python ${PYTHON_BINARY_DIR})
  _symlink_tree(${TARGET_NAME}_setup_tree ${PYTHON_SETUP_DIR}/setup.py ${PYTHON_BINARY_DIR})
  # Needed for scripts/katana_version
  _symlink_tree(${TARGET_NAME}_scripts_tree ${PYTHON_SETUP_DIR}/scripts ${PYTHON_BINARY_DIR})

  _generate_build_configuration_ini(FILE_PREFIX ${TARGET_NAME}_ DEPENDS ${X_DEPENDS})

  # TODO(amp): this only supports the environment variable CMAKE_BUILD_PARALLEL_LEVEL as a way to do parallel builds.
  #  and CMAKE_BUILD_PARALLEL_LEVEL must be set for both cmake configure and build phases.
  #  -j will not propogate into python builds and if CMAKE_BUILD_PARALLEL_LEVEL varies then only some things will be
  #  in parallel. Parallel or not will not affect correctness however.

  if (ENV{CMAKE_BUILD_PARALLEL_LEVEL})
    set(parallel --parallel $ENV{CMAKE_BUILD_PARALLEL_LEVEL})
  endif()
  set(quiet "")
  #  set(quiet --quiet)
  #  if (ENV{VERBOSE} OR ENV{CMAKE_VERBOSE_MAKEFILE})
  #    set(quiet "")
  #  endif()

  set(PYTHONPATH "$ENV{PYTHONPATH}")

  foreach(dep IN LISTS X_DEPENDS)
    get_target_property(dir ${dep} PYTHON_BINARY_DIR)
    if (dir)
      string(APPEND PYTHONPATH ":${dir}/python")
    endif()
  endforeach()

  # Command used to launch setup.py
  set(PYTHON_SETUP_COMMAND
      ${CMAKE_COMMAND} -E env
      "PYTHONPATH=${PYTHONPATH}"
      # Reference generated ini files which contain compiler flags, etc.
      "KATANA_CXX_CONFIG=${CMAKE_CURRENT_BINARY_DIR}/${TARGET_NAME}_CXX.ini"
      "KATANA_C_CONFIG=${CMAKE_CURRENT_BINARY_DIR}/${TARGET_NAME}_C.ini"
      # Pass katana version
      "KATANA_VERSION=${KATANA_VERSION}"
      "KATANA_COPYRIGHT_YEAR=${KATANA_COPYRIGHT_YEAR}"
      # Finally, launch setup.py
      ${Python3_EXECUTABLE} setup.py)

  add_custom_target(
      ${TARGET_NAME}
      ALL
      COMMAND ${PYTHON_SETUP_COMMAND} ${quiet} build "$<$<CONFIG:Debug>:--debug>" ${parallel}
      COMMAND install ${PYTHON_ENV_SCRIPT}.tmp ${PYTHON_ENV_SCRIPT}
      BYPRODUCTS ${PYTHON_BINARY_DIR} ${PYTHON_ENV_SCRIPT}.tmp ${PYTHON_ENV_SCRIPT}
      WORKING_DIRECTORY ${PYTHON_BINARY_DIR}
      COMMENT "Building ${TARGET_NAME} in symlink tree ${PYTHON_BINARY_DIR}"
  )

  add_dependencies(${TARGET_NAME} ${TARGET_NAME}_python_tree ${TARGET_NAME}_setup_tree ${TARGET_NAME}_scripts_tree)
  if(X_DEPENDS)
    add_dependencies(${TARGET_NAME} ${X_DEPENDS})
  endif()

  # TODO(amp): The RPATH of the installed python modules will contain a reference to the build
  #  directory. This is not ideal and could cause confusion, but without depending on an
  #  additional command line tool there isn't any real way to fix it.
  install(
      CODE "execute_process(
                COMMAND
                  ${PYTHON_SETUP_COMMAND} install --skip-build --prefix=\$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}
                WORKING_DIRECTORY ${PYTHON_BINARY_DIR}
                COMMAND_ERROR_IS_FATAL ANY)"
      COMPONENT python)

  set_target_properties(${TARGET_NAME} PROPERTIES
                        PYTHON_ENV_SCRIPT "${PYTHON_ENV_SCRIPT}"
                        PYTHON_BINARY_DIR "${PYTHON_BINARY_DIR}"
                        PYTHON_SETUP_COMMAND "${PYTHON_SETUP_COMMAND}")

  set(ENV_SCRIPT_STR "#!/bin/sh\n")
  foreach(dep IN LISTS X_DEPENDS TARGET_NAME)
    get_target_property(dir ${dep} PYTHON_BINARY_DIR)
    if (dir)
      string(APPEND ENV_SCRIPT_STR "\
for f in ${dir}/build/lib.*; do
  export PYTHONPATH=$f\${PYTHONPATH:+:\$PYTHONPATH}
done
")
    else()
      # No need to set LD_LIBRARY_PATH under the assumption that rpaths are set correctly.
      #      string(APPEND ENV_SCRIPT_STR "\
      #export LD_LIBRARY_PATH=\"$<TARGET_LINKER_FILE_DIR:${dep}>\${LD_LIBRARY_PATH:+:\$LD_LIBRARY_PATH}\"
      #")
      #echo LD_LIBRARY_PATH=\$LD_LIBRARY_PATH
    endif()
  endforeach()

  string(APPEND ENV_SCRIPT_STR "
echo PYTHONPATH=\$PYTHONPATH
\"$@\"
")
  file(GENERATE OUTPUT ${PYTHON_ENV_SCRIPT}.tmp CONTENT "${ENV_SCRIPT_STR}")

  if(NOT TARGET python)
    add_custom_target(python)
  endif()
  add_dependencies(python ${TARGET_NAME})
endfunction()

function(add_python_setuptools_tests TARGET_NAME)
  set(no_value_options NOT_QUICK)
  set(one_value_options PATH)
  set(multi_value_options)

  cmake_parse_arguments(X "${no_value_options}" "${one_value_options}" "${multi_value_options}" ${ARGN})

  get_target_property(script ${TARGET_NAME} PYTHON_ENV_SCRIPT)
  add_test(NAME ${TARGET_NAME}
           COMMAND ${script} pytest -v ${X_PATH}
           WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR})
  if(NOT X_NOT_QUICK)
    set_tests_properties(${TARGET_NAME} PROPERTIES LABELS "quick;python")
  else()
    set_tests_properties(${TARGET_NAME} PROPERTIES LABELS python)
  endif()
endfunction()

add_custom_target(python_docs)

function(add_python_setuptools_docs TARGET_NAME)
  set(no_value_options)
  set(one_value_options)
  set(multi_value_options)

  cmake_parse_arguments(X "${no_value_options}" "${one_value_options}" "${multi_value_options}" ${ARGN})

  get_target_property(PYTHON_SETUP_COMMAND ${TARGET_NAME} PYTHON_SETUP_COMMAND)
  get_target_property(PYTHON_BINARY_DIR ${TARGET_NAME} PYTHON_BINARY_DIR)

  if (PY_SPHINX)
    add_custom_target(
        ${TARGET_NAME}_docs
        COMMAND ${CMAKE_COMMAND} -E rm -rf ${PYTHON_BINARY_DIR}/build/sphinx
        COMMAND ${PYTHON_SETUP_COMMAND} build_sphinx
        COMMAND ${CMAKE_COMMAND} -E copy_directory ${PYTHON_BINARY_DIR}/build/sphinx/html ${CMAKE_BINARY_DIR}/docs/${TARGET_NAME}
        COMMAND ${CMAKE_COMMAND} -E echo "${TARGET_NAME} documentation at file://${CMAKE_BINARY_DIR}/docs/${TARGET_NAME}/index.html"
        BYPRODUCTS ${PYTHON_BINARY_DIR}/build/sphinx
        WORKING_DIRECTORY ${PYTHON_BINARY_DIR}
        COMMENT "Building ${TARGET_NAME} sphinx documentation in symlink tree ${PYTHON_BINARY_DIR}"
    )
    add_dependencies(${TARGET_NAME}_docs ${TARGET_NAME})
    add_dependencies(python_docs ${TARGET_NAME}_docs)
  endif()
endfunction()
