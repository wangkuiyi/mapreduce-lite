
# Build target
set(LIBS mapreduce_lite sorted_buffer strutil hash base event_core rt protobuf system gflags boost_thread boost_system boost_filesystem pthread)

add_executable(mrl-wordcount wordcount.cc)
target_link_libraries(mrl-wordcount ${LIBS})

#add_executable(mrl-wordcount.maponly map_only.cc)
#target_link_libraries(mrl-wordcount.maponly ${LIBS})

# Install library and header files
install(TARGETS mrl-wordcount DESTINATION mrlite-demo)

set(DEMO_FILES
  ${CMAKE_CURRENT_SOURCE_DIR}/README
  ${CMAKE_CURRENT_SOURCE_DIR}/wordcount.cc
  ${CMAKE_CURRENT_SOURCE_DIR}/Makefile
  ${CMAKE_CURRENT_SOURCE_DIR}/Makefile.rules
  ${CMAKE_CURRENT_SOURCE_DIR}/wordcount_run_locally.sh
  ${CMAKE_CURRENT_SOURCE_DIR}/wordcount_batch_reduction_mode.sh
  ${CMAKE_CURRENT_SOURCE_DIR}/wordcount_incremental_reduction_mode.sh
  )
set(TESTDATA_FILES
  ${CMAKE_CURRENT_SOURCE_DIR}/testdata/text-00000-of-00002
  ${CMAKE_CURRENT_SOURCE_DIR}/testdata/text-00001-of-00002
  )
install(FILES ${DEMO_FILES} DESTINATION mrlite-demo)
install(FILES ${TESTDATA_FILES} DESTINATION mrlite-demo/testdata)
