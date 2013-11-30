# Build library strutil.
add_library(hash md5_hash.cc simple_hash.cc)

# Build unittests.
set(LIBS base hash gtest pthread)

add_executable(md5_hash_test md5_hash_test.cc)
target_link_libraries(md5_hash_test gtest_main ${LIBS})

# Install library and header files
install(TARGETS hash DESTINATION lib/paralgo)
FILE(GLOB HEADER_FILES "${CMAKE_CURRENT_SOURCE_DIR}/*.h")
install(FILES ${HEADER_FILES} DESTINATION include/paralgo/hash)
