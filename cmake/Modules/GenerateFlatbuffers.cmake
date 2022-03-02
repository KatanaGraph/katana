# create a new target that generates flat buffer files from `fb_files`
# and set `output_var` to the list of generated file names (suitable for
# inclusion int a sources list)
function(generate_cpp_flatbuffers output_var fb_files)
  foreach(file ${fb_files})
    get_filename_component(output ${file} NAME_WE)
    set(output "${output}_generated.h")
    list(APPEND fb_cpp ${output})
  endforeach()

  # TODO (witchel), does this work for a list of fb_cpp?
  add_custom_command(
    OUTPUT ${fb_cpp}
    COMMAND "${FLATC}"
    --gen-all --binary --gen-object-api --scoped-enums --cpp -o "${CMAKE_CURRENT_BINARY_DIR}/" ${fb_files}
    DEPENDS ${fb_files}
    WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
    COMMENT "Generating C++ bindings for ${fb_files}"
  )

  set(${output_var} ${fb_cpp} PARENT_SCOPE)
endfunction()
