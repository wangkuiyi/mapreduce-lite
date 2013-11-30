/*
This program is free software; you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation; either version 2 of the License, or (at
your option) any later version.

This program is distributed in the hope that it will be useful, but
WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program; if not, write to the Free Software
Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
02110-1301, USA.
*/
// Copyright 2010, Tencent Inc.
// Author: Yi Wang (yiwang@tencent.com)
//         Hangjun Ye (hansye@tencent.com)
//
// Defines several base class and registers for testing. We intentionally
// define them in a separate file as some compilers don't correctly support to
// define static variable in inline function, they create a separate copy each
// time it's included. We want to make sure it doesn't happen to us.

#ifndef COMMON_BASE_CLASS_REGISTER_HELPER_H_
#define COMMON_BASE_CLASS_REGISTER_HELPER_H_

#include <string>
#include "src/base/class_register.h"

class Mapper {
 public:
  Mapper() {}
  virtual ~Mapper() {}

  virtual std::string GetMapperName() const = 0;
};

CLASS_REGISTER_DEFINE_REGISTRY(mapper_register, Mapper);

#define REGISTER_MAPPER(mapper_name)                            \
  CLASS_REGISTER_OBJECT_CREATOR(                                \
      mapper_register, Mapper, #mapper_name, mapper_name)

#define CREATE_MAPPER(mapper_name_as_string)                            \
  CLASS_REGISTER_CREATE_OBJECT(mapper_register, mapper_name_as_string)

CLASS_REGISTER_DEFINE_REGISTRY(second_mapper_register, Mapper);

#define REGISTER_SECONDARY_MAPPER(mapper_name)                          \
  CLASS_REGISTER_OBJECT_CREATOR(                                        \
      second_mapper_register, Mapper, #mapper_name, mapper_name)

#define CREATE_SECONDARY_MAPPER(mapper_name_as_string)  \
  CLASS_REGISTER_CREATE_OBJECT(second_mapper_register,  \
                               mapper_name_as_string)


class Reducer {
 public:
  Reducer() {}
  virtual ~Reducer() {}

  virtual std::string GetReducerName() const = 0;
};

CLASS_REGISTER_DEFINE_REGISTRY(reducer_register, Reducer);

#define REGISTER_REDUCER(reducer_name)                          \
  CLASS_REGISTER_OBJECT_CREATOR(                                \
      reducer_register, Reducer, #reducer_name, reducer_name)

#define CREATE_REDUCER(reducer_name_as_string)                          \
  CLASS_REGISTER_CREATE_OBJECT(reducer_register, reducer_name_as_string)



class FileImpl {
 public:
  FileImpl() {}
  virtual ~FileImpl() {}

  virtual std::string GetFileImplName() const = 0;
};

CLASS_REGISTER_DEFINE_REGISTRY(file_impl_register, FileImpl);

#define REGISTER_DEFAULT_FILE_IMPL(file_impl_name)      \
  CLASS_REGISTER_DEFAULT_OBJECT_CREATOR(                \
      file_impl_register, FileImpl, file_impl_name)

#define REGISTER_FILE_IMPL(path_prefix_as_string, file_impl_name)       \
  CLASS_REGISTER_OBJECT_CREATOR(                                        \
      file_impl_register, FileImpl, path_prefix_as_string, file_impl_name)

#define CREATE_FILE_IMPL(path_prefix_as_string)                         \
  CLASS_REGISTER_CREATE_OBJECT(file_impl_register, path_prefix_as_string)

#endif  // COMMON_BASE_CLASS_REGISTER_HELPER_H_
