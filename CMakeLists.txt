# Copyright 2010 Tencent Inc.
# Author: Yi Wang (yiwang@tencent.com)
#
# This is the root CMakeLists.txt file in the mapreduce-lite project,
# which build all subdirectories (packages) in the order of
# inter-package dependence.  You might not have ``check-out'' access
# to all these packages.  If os, please just comment out lines
# referring to packages that you cannot access.  But please confirm
# with the source management administrator that once you have access
# to package A, you must also have access to all packages on which A
# depends.
#
# Before you can build mapreduce-lite, you need to build and install
# dependent libraries following
#
#   http://code.google.com/p/mapreduce-lite/wiki/BuildMapReduceLite.
#
# This wiki page assumes you installed all 3rd-party libraries into a
# directory named /home/you/3rd-party.  You need to use that directory
# to replace the directory in the following variable setting:
#
#   set(THIRD_PARTY_DIR "/Users/wangyi/thirdparty")
#
# Now, you can build the mapreduce-lite using the following commands:
#
#  $> mkdir build
#  $> cd build
#  $> cmake ..
#  $> make
#
# Thus you check out the mapreduce-lite project and build it in a
# subdirectory ``build''.  If you want further to install the built
# project, you can modify the default installation directory:
#
#   set(CMAKE_INSTALL_PREFIX "/home/public/mapreduce-lite")
#
# and type the command
#
#  $> make install
#
# If you want to use distcc for a distributed build, substitute above
# command
#   cmake ../mapreduce-lite
# by
#   CC=distcc cmake ../mapreduce-lite
#
project ("mapreduce_lite")

cmake_minimum_required(VERSION 2.8)  # Requires 2.8 for protobuf support.

#------------------------------------------------------------------------------
# Add protobuf compilation support
#------------------------------------------------------------------------------
include("FindProtobuf")
find_package(Protobuf REQUIRED)

#------------------------------------------------------------------------------
# Take almost all warnining;
# Take warnings as errors;
# Do not generate debug symbols;
# Optimization level 2;
#------------------------------------------------------------------------------
add_definitions(" -Wall -Wno-sign-compare -Werror -O2 ")

#------------------------------------------------------------------------------
# Declare where our project will be installed.
#------------------------------------------------------------------------------
set(CMAKE_INSTALL_PREFIX "/home/wyi/mapreduce-lite")

#------------------------------------------------------------------------------
# Ensure executables are statically linked with libraries.
#------------------------------------------------------------------------------
if(NOT ${CMAKE_SYSTEM_NAME} MATCHES "Darwin")
  set(CMAKE_EXE_LINKER_FLAGS "-static -static-libgcc")
endif()

#------------------------------------------------------------------------------
# Set include paths and library paths.
#
# MapReduce Lite depends on the following thirdparty libraries:
#
#  - boost
#  - gflags
#  - libevent
#
# You can install them using package management tools on your system,
# or build them from source by yourself.  In either way, you need to
# tell where these libraries were installed.  For example, if you
# build all libraries and install them to /home/wyi/3rd-party, you
# might need the following commands:
#
#  set(THIRD_PARTY_DIR "/home/wyi/3rd-party")
#  set(BOOST_DIR "${THIRD_PARTY_DIR}/boost")
#  set(GFLAGS_DIR "${THIRD_PARTY_DIR}/gflags")
#  set(LIBEVENT_DIR "${THIRD_PARTY_DIR}/libevent")
#
#  include_directories(
#   "${BOOST_DIR}/include"
#   "${GFLAGS_DIR}/include"
#   "${LIBEVENT_DIR}/include"
#   )
#
# link_directories(
#   "${BOOST_DIR}/lib"
#   "${GFLAGS_DIR}/lib"
#   "${LIBEVENT_DIR}/lib"
#   )
#
# On MacOS X, Homebrew installs header files to /usr/local/include and
# libraries to /usr/local/lib.
#
# MapReduce Lite uses googletest framework for unit testing.  As it is
# not recommended to build googletest as a system-wide library, we
# must import googletest into our source tree before building
# MapReduce Lite.
#
#  1. Download or checkout googletest code  from code.google.com/p/googletest.
#  2. Create a symbolic link named `src/gtest` pointing to googletest code.
# ------------------------------------------------------------------------------
include_directories(
  "${PROJECT_SOURCE_DIR}"
  "${PROJECT_BINARY_DIR}"
  "${PROTOBUF_INCLUDE_DIRS}"
  "${PROJECT_SOURCE_DIR}/src/gtest/include"
  "${BOOST_DIR}/include"
  "${GFLAGS_DIR}/include"
  "${LIBEVENT_DIR}/include"
  )

link_directories(
  "${PROJECT_BINARY_DIR}/src/base"
  "${PROJECT_BINARY_DIR}/src/strutil"
  "${PROJECT_BINARY_DIR}/src/hash"
  "${PROJECT_BINARY_DIR}/src/system"
  "${PROJECT_BINARY_DIR}/src/mapreduce_lite"
  "${BOOST_DIR}/lib"
  "${GFLAGS_DIR}/lib"
  "${GTEST_DIR}/lib"
  "${LIBEVENT_DIR}/lib"
  )

#------------------------------------------------------------------------------
# Declare packages in paralgo project.
#------------------------------------------------------------------------------
add_subdirectory(src/gtest)
add_subdirectory(src/base)
add_subdirectory(src/strutil)
add_subdirectory(src/hash)
add_subdirectory(src/system)
add_subdirectory(src/sorted_buffer)
add_subdirectory(src/mapreduce_lite)
