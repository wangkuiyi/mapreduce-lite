# Build library strutil.
add_library(sorted_buffer memory_allocator.cc memory_piece.cc sorted_buffer.cc sorted_buffer_iterator.cc)

# Build unittests.
set(LIBS sorted_buffer strutil base protobuf boost_program_options boost_regex boost_filesystem boost_system gtest pthread)

add_executable(memory_allocator_test memory_allocator_test.cc)
target_link_libraries(memory_allocator_test gtest_main ${LIBS})

add_executable(memory_piece_less_than_test memory_piece_less_than_test.cc)
target_link_libraries(memory_piece_less_than_test gtest_main ${LIBS})

add_executable(sorted_buffer_test sorted_buffer_test.cc)
target_link_libraries(sorted_buffer_test gtest_main ${LIBS})

add_executable(memory_piece_io_test memory_piece_io_test.cc)
target_link_libraries(memory_piece_io_test gtest_main ${LIBS})

add_executable(memory_piece_test memory_piece_test.cc)
target_link_libraries(memory_piece_test gtest_main ${LIBS})

add_executable(sorted_buffer_iterator_test sorted_buffer_iterator_test.cc)
target_link_libraries(sorted_buffer_iterator_test gtest_main ${LIBS})

add_executable(sorted_buffer_regression_test sorted_buffer_regression_test.cc)
target_link_libraries(sorted_buffer_regression_test gtest_main ${LIBS})

# Install library and header files
install(TARGETS sorted_buffer DESTINATION lib/paralgo)
FILE(GLOB HEADER_FILES "${CMAKE_CURRENT_SOURCE_DIR}/*.h")
install(FILES ${HEADER_FILES} DESTINATION include/paralgo/sorted_buffer)
