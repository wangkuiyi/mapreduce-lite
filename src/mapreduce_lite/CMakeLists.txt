# Build protobuf
protobuf_generate_cpp(PROTO_SRCS PROTO_HDRS protofile.proto)

# Build library mapreduce_lite.
add_library(mapreduce_lite ${PROTO_SRCS} flags.cc mapreduce_lite.cc mapreduce_main.cc protofile.cc reader.cc signaling_queue.cc socket_communicator.cc tcp_socket.cc)

set(LIBS mapreduce_lite sorted_buffer strutil hash base event_core protobuf system gflags gtest boost_thread-mt boost_filesystem boost_system pthread)

# Build unittests.
add_executable(protofile_test protofile_test.cc)
target_link_libraries(protofile_test gtest_main ${LIBS})

add_executable(signaling_queue_test signaling_queue_test.cc)
target_link_libraries(signaling_queue_test gtest_main ${LIBS})

add_executable(utils_test utils_test.cc)
target_link_libraries(utils_test gtest_main ${LIBS})

add_executable(socket_communicator_test socket_communicator_test.cc)
target_link_libraries(socket_communicator_test gtest_main ${LIBS})

add_executable(tcp_socket_test tcp_socket_test.cc)
target_link_libraries(tcp_socket_test gtest_main ${LIBS})

add_executable(reader_test reader_test.cc)
target_link_libraries(reader_test gtest_main ${LIBS})

add_executable(registerer_test registerer_test.cc)
target_link_libraries(registerer_test gtest_main ${LIBS})

# Install library and header files
install(TARGETS mapreduce_lite DESTINATION lib/paralgo)
FILE(GLOB HEADER_FILES "${CMAKE_CURRENT_SOURCE_DIR}/*.h")
FILE(GLOB PYTHON_FILES "${CMAKE_CURRENT_SOURCE_DIR}/scheduler/*.py")
install(FILES ${HEADER_FILES} DESTINATION include/paralgo/mapreduce_lite)
install(FILES ${PYTHON_FILES} DESTINATION scheduler)

# Build demo
add_subdirectory(demo)
