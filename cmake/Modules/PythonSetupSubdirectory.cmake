include_guard(DIRECTORY)

function(_symlink_tree TARGET_NAME SOURCE DEST)
  if(NOT IS_ABSOLUTE ${SOURCE})
    set(SOURCE ${CMAKE_CURRENT_SOURCE_DIR}/${SOURCE})
  endif()

  if(NOT IS_ABSOLUTE ${DEST})
    set(SOURCE ${CMAKE_CURRENT_BINARY_DIR}/${DEST})
  endif()

  if(IS_DIRECTORY ${SOURCE})
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

# Construct a text file containing the options used to build a library that
# depends on DEPENDS. CMake only knows the full flag sets at generation time,
# meaning that we must generate a file and cannot just place things in
# variables.
#
# The format of the text file is simply lines of KEY=VALUE with lines of
# whitespace skipped.
#
# CMake (3.17+) can generate linker arguments with LINKER: and/or SHELL:
# prefixes. CMake internally desugars these with
# CMAKE_<lang>_LINKER_WRAPPER_FLAG and CMAKE_<lang>_LINKER_WRAPPER_FLAG_SEP.
# However, there is no way to invoke that from a generator expression, so the
# desugaring described in
# https://cmake.org/cmake/help/latest/command/target_link_options.html is
# reimplemented in katana_setup.py when the JSON file is read.
function(_generate_build_configuration_txt)
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

  # Add global variables which contribute to build for each potential build config
  foreach (CONFIG_NAME "" "DEBUG" "RELEASE" "MINSIZEREL" "RELWITHDEBINFO")
    if (CONFIG_NAME)
      set(CONFIG_SUFFIX "_${CONFIG_NAME}")
      set(CONFIG_PRED "$<CONFIG:${CONFIG_NAME}>")
    else()
      set(CONFIG_SUFFIX "")
      set(CONFIG_PRED "1")
    endif()

    # The variables contain space separated arguments, so convert them into ;-separated lists.
    string(REPLACE " " ";" CMAKE_CXX_FLAGS${CONFIG_SUFFIX}_LIST "${CMAKE_CXX_FLAGS${CONFIG_SUFFIX}}")
    string(REPLACE " " ";" CMAKE_C_FLAGS${CONFIG_SUFFIX}_LIST "${CMAKE_C_FLAGS${CONFIG_SUFFIX}}")
    list(APPEND COMPILE_OPTIONS "$<${CONFIG_PRED}:$<$<COMPILE_LANGUAGE:CXX>:${CMAKE_CXX_FLAGS${CONFIG_SUFFIX}_LIST}>>")
    list(APPEND COMPILE_OPTIONS "$<${CONFIG_PRED}:$<$<COMPILE_LANGUAGE:C>:${CMAKE_C_FLAGS${CONFIG_SUFFIX}_LIST}>>")

    string(REPLACE " " ";" CMAKE_LINK_FLAGS${CONFIG_SUFFIX}_LIST "${CMAKE_LINK_FLAGS${CONFIG_SUFFIX}}")
    list(APPEND LINK_OPTIONS "$<${CONFIG_PRED}:${CMAKE_LINK_FLAGS${CONFIG_SUFFIX}_LIST}>")
  endforeach ()

  if (CMAKE_CXX_STANDARD)
    list(APPEND COMPILE_OPTIONS "$<$<COMPILE_LANGUAGE:CXX>:-std=c++${CMAKE_CXX_STANDARD}>")
  endif ()

  foreach(TARGET_NAME IN LISTS X_DEPENDS)
    get_target_property(X ${TARGET_NAME} PYTHON_ENV_SCRIPT)
    if (X)
      continue()
    endif()
    list(APPEND INCLUDE_DIRECTORIES "$<TARGET_GENEX_EVAL:${TARGET_NAME},$<TARGET_PROPERTY:${TARGET_NAME},INTERFACE_INCLUDE_DIRECTORIES>>")
    list(APPEND COMPILE_DEFINITIONS "$<TARGET_GENEX_EVAL:${TARGET_NAME},$<TARGET_PROPERTY:${TARGET_NAME},INTERFACE_COMPILE_DEFINITIONS>>")
    # TODO(amp): Including INTERFACE_LINK_OPTIONS causes an unavoidable cmake generation failure in cmake 3.20+ due to the use of $<HOST_LINK:...>.
    #  INTERFACE_LINK_OPTIONS doesn't seem to be required for the dependencies we are using right now, so just skip it for the moment.
    # list(APPEND LINK_OPTIONS "$<TARGET_GENEX_EVAL:${TARGET_NAME},$<TARGET_PROPERTY:${TARGET_NAME},INTERFACE_LINK_OPTIONS>>")
    list(APPEND LINK_OPTIONS "$<TARGET_GENEX_EVAL:${TARGET_NAME},LINKER:-rpath=$<TARGET_LINKER_FILE_DIR:${TARGET_NAME}>>")
    list(APPEND LINK_OPTIONS "$<TARGET_GENEX_EVAL:${TARGET_NAME},LINKER:-rpath=$<JOIN:$<TARGET_PROPERTY:${TARGET_NAME},BUILD_RPATH>,;LINKER:-rpath=>>")
    list(APPEND LINK_OPTIONS "$<TARGET_GENEX_EVAL:${TARGET_NAME},LINKER:-rpath=$<JOIN:$<TARGET_PROPERTY:${TARGET_NAME},INSTALL_RPATH>,;LINKER:-rpath=>>")
    list(APPEND LINK_OPTIONS "$<TARGET_GENEX_EVAL:${TARGET_NAME},$<TARGET_LINKER_FILE:${TARGET_NAME}>>")
    list(APPEND LINK_OPTIONS "$<TARGET_GENEX_EVAL:${TARGET_NAME},$<TARGET_PROPERTY:${TARGET_NAME},INTERFACE_LINK_DEPENDS>>")
    list(APPEND LINK_OPTIONS "$<TARGET_GENEX_EVAL:${TARGET_NAME},$<TARGET_PROPERTY:${TARGET_NAME},LINK_DEPENDS>>")
    list(APPEND COMPILE_OPTIONS "$<TARGET_GENEX_EVAL:${TARGET_NAME},$<TARGET_PROPERTY:${TARGET_NAME},INTERFACE_COMPILE_OPTIONS>>")
  endforeach()

  set(COMPILER
      "$<$<COMPILE_LANGUAGE:CXX>:${CMAKE_CXX_COMPILER_LAUNCHER};${CMAKE_CXX_COMPILER}>$<$<COMPILE_LANGUAGE:C>:${CMAKE_C_COMPILER_LAUNCHER};${CMAKE_C_COMPILER}>")

  # TODO(amp): It might should be possible to use generator expressions to build actual JSON lists instead of string
  #  containing cmake lists. However it's not clear this is actually better.
  file(GENERATE OUTPUT ${X_FILE_PREFIX}$<COMPILE_LANGUAGE>.txt
       CONTENT "
COMPILER=${COMPILER}
INCLUDE_DIRECTORIES=$<REMOVE_DUPLICATES:${INCLUDE_DIRECTORIES}>
COMPILE_DEFINITIONS=$<REMOVE_DUPLICATES:${COMPILE_DEFINITIONS}>
LINK_OPTIONS=${LINK_OPTIONS}
COMPILE_OPTIONS=${COMPILE_OPTIONS}
LINKER_WRAPPER_FLAG=$<$<COMPILE_LANGUAGE:CXX>:${CMAKE_CXX_LINKER_WRAPPER_FLAG}>$<$<COMPILE_LANGUAGE:C>:${CMAKE_C_LINKER_WRAPPER_FLAG}>
LINKER_WRAPPER_FLAG_SEP=$<$<COMPILE_LANGUAGE:CXX>:${CMAKE_CXX_LINKER_WRAPPER_FLAG_SEP}>$<$<COMPILE_LANGUAGE:C>:${CMAKE_C_LINKER_WRAPPER_FLAG_SEP}>
")
endfunction()


# Build a python project as part of the cmake build.
#
# This attempts to pass all compiler and linker flags to setuptools in the python project. However,
# CMake provides no way to directly get compiler or linker flags, so we need to reimplement the
# logic by looking at the properties of our dependencies and putting things together. This is not
# perfect, but works in most cases.
#
# Because of these issues with CMake, we also don't support transitive dependencies since there
# is no way to trace the dependencies from CMake code. So ALL dependencies need to be provided
# even if they "should" be implied by transitive dependencies.
function(add_python_setuptools_target TARGET_NAME)
  set(no_value_options)
  set(one_value_options SETUP_DIRECTORY COMPONENT)
  set(multi_value_options DEPENDS)

  cmake_parse_arguments(X "${no_value_options}" "${one_value_options}" "${multi_value_options}" ${ARGN})

  set(PYTHON_SETUP_DIR ${X_SETUP_DIRECTORY})
  if(NOT PYTHON_SETUP_DIR)
    set(PYTHON_SETUP_DIR ${CMAKE_CURRENT_SOURCE_DIR})
  endif()

  if(NOT X_COMPONENT)
    set(X_COMPONENT python)
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

  # TODO(amp): this only supports the environment variable CMAKE_BUILD_PARALLEL_LEVEL as a way to do parallel builds.
  #  and CMAKE_BUILD_PARALLEL_LEVEL must be set for both cmake configure and build phases.
  #  -j will not propagate into python builds and if CMAKE_BUILD_PARALLEL_LEVEL varies then only some things will be
  #  in parallel. Parallel or not will not affect correctness however.

  if (DEFINED ENV{CMAKE_BUILD_PARALLEL_LEVEL})
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
    if(dir)
      string(APPEND PYTHONPATH ":${dir}/python")
    endif()
  endforeach()

  # Command used to launch setup.py
  set(PYTHON_SETUP_COMMAND
      ${CMAKE_COMMAND} -E env
      "PYTHONPATH=${PYTHONPATH}"
      # Reference generated json files which contain compiler flags, etc.
      "KATANA_CXX_CONFIG=${CMAKE_CURRENT_BINARY_DIR}/${TARGET_NAME}_CXX.txt"
      "KATANA_C_CONFIG=${CMAKE_CURRENT_BINARY_DIR}/${TARGET_NAME}_C.txt"
      # Pass katana version
      "KATANA_VERSION=${KATANA_VERSION}"
      "KATANA_COPYRIGHT_YEAR=${KATANA_COPYRIGHT_YEAR}"
      "KATANA_SETUP_REQUIREMENTS_CACHE=${CMAKE_BINARY_DIR}/katana_setup_requirements_cache.txt"
      # Finally, launch setup.py
      ${Python3_EXECUTABLE} setup.py)
  # Write the python setup.py command to a file similar to link.txt to aid in build debugging
  file(GENERATE OUTPUT ${PYTHON_BINARY_DIR}/python_setup.txt CONTENT "$<JOIN:${PYTHON_SETUP_COMMAND}, >")

  add_custom_target(
      ${TARGET_NAME}
      ALL
      COMMAND ${PYTHON_SETUP_COMMAND} ${quiet} build "$<$<CONFIG:Debug>:--debug>" ${parallel}
      COMMAND install ${PYTHON_ENV_SCRIPT}.tmp ${PYTHON_ENV_SCRIPT}
      BYPRODUCTS ${PYTHON_BINARY_DIR} ${PYTHON_ENV_SCRIPT}
      WORKING_DIRECTORY ${PYTHON_BINARY_DIR}
      COMMENT "Building ${TARGET_NAME} in symlink tree ${PYTHON_BINARY_DIR}"
  )

  add_custom_target(
      ${TARGET_NAME}_wheel
      COMMAND ${PYTHON_SETUP_COMMAND} ${quiet} bdist_wheel --dist-dir ${CMAKE_BINARY_DIR}/pkg
      WORKING_DIRECTORY ${PYTHON_BINARY_DIR}
      COMMENT "bdist ${TARGET_NAME} in symlink tree ${PYTHON_BINARY_DIR}"
  )
  add_dependencies(${TARGET_NAME}_wheel ${TARGET_NAME})

  # When building with Ninja, Ninja refuses to delete non-empty directories when cleaning.
  # Thus ${PYTHON_BINARY_DIR} is included here, which explicitly removes the directory for the clean target.
  set_property(
      TARGET ${TARGET_NAME}
      APPEND
      PROPERTY ADDITIONAL_CLEAN_FILES ${PYTHON_BINARY_DIR} ${CMAKE_BINARY_DIR}/katana_setup_requirements_cache.txt
  )

  add_dependencies(${TARGET_NAME} ${TARGET_NAME}_python_tree ${TARGET_NAME}_setup_tree ${TARGET_NAME}_scripts_tree)
  if(X_DEPENDS)
    add_dependencies(${TARGET_NAME} ${X_DEPENDS})
  endif()

  _generate_build_configuration_txt(FILE_PREFIX ${TARGET_NAME}_ DEPENDS ${X_DEPENDS})

  # TODO(amp): The RPATH of the installed python modules will contain a reference to the build
  # directory. This is not ideal and could cause confusion, but without depending on an
  # additional command line tool there isn't any real way to fix it.
  install(
      CODE "execute_process(
                COMMAND
                    ${PYTHON_SETUP_COMMAND} install --prefix=\$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}
                WORKING_DIRECTORY ${PYTHON_BINARY_DIR}
                RESULT_VARIABLE EXIT_CODE)
            if(EXIT_CODE)
              message(FATAL_ERROR \"python install failed: code \${EXIT_CODE}\")
            endif()"
      COMPONENT ${X_COMPONENT})

  set_target_properties(
      ${TARGET_NAME} PROPERTIES
      PYTHON_ENV_SCRIPT "${PYTHON_ENV_SCRIPT}"
      PYTHON_BINARY_DIR "${PYTHON_BINARY_DIR}"
      PYTHON_SETUP_COMMAND "${PYTHON_SETUP_COMMAND}")

  set(ENV_SCRIPT_STR "#!/bin/sh\n")
  foreach(dep IN LISTS X_DEPENDS TARGET_NAME)
    get_target_property(dir ${dep} PYTHON_BINARY_DIR)
    if(dir)
      # ${dir}/build/lib* is the main build and library directory and contains
      # all the executable extensions and python code. ${dir}/python is the
      # temporary "source" for setuptools that is polluted with setuptools
      # output files. ${dir}/build/lib* is used for loading modules and MUST be
      # before the matching "source" directory to make sure extensions are
      # found. ${dir}/python is included for the setuptools generate `.egg-info`
      # directory which enables Python package metadata which is used for Python
      # Katana plugins.
      string(APPEND ENV_SCRIPT_STR "\
for f in ${dir}/build/lib* ${dir}/python; do
  python_path_additions=\${python_path_additions:+\$python_path_additions:}$f
done
if [ \"$python_path_additions\" ]; then
  export PYTHONPATH=$python_path_additions\${PYTHONPATH:+:\$PYTHONPATH}
fi
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
if [ \"$#\" -eq 0 ]; then
  echo PYTHONPATH=\$PYTHONPATH 1>&2
fi
exec \"$@\"
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
           COMMAND ${script} pytest -s -v --import-mode append ${X_PATH}
           WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR})
  set_property(TEST ${TARGET_NAME} APPEND
               PROPERTY ENVIRONMENT KATANA_SOURCE_DIR=${CMAKE_CURRENT_SOURCE_DIR})
  if(NOT X_NOT_QUICK)
    set_tests_properties(${TARGET_NAME} PROPERTIES LABELS "quick;python")
  else()
    set_tests_properties(${TARGET_NAME} PROPERTIES LABELS python)
  endif()
endfunction()
