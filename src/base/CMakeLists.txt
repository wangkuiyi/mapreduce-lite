# Build library strutil.
add_library(base logging.cc random.cc varint32.cc stream_wrapper.cc)

# Build unittests.
set(LIBS base protobuf gflags gtest pthread)

add_executable(class_register_test class_register_test.cc class_register_test_helper.cc)
target_link_libraries(class_register_test gtest_main ${LIBS})

add_executable(cvector_test cvector_test.cc)
target_link_libraries(cvector_test gtest_main ${LIBS})

add_executable(stl-util_test stl-util_test.cc)
target_link_libraries(stl-util_test gtest_main ${LIBS})

add_executable(varint32_test varint32_test.cc)
target_link_libraries(varint32_test gtest_main ${LIBS})

# Install library and header files
install(TARGETS base DESTINATION lib/paralgo)
FILE(GLOB HEADER_FILES "${CMAKE_CURRENT_SOURCE_DIR}/*.h")
install(FILES ${HEADER_FILES} DESTINATION include/paralgo/base)
