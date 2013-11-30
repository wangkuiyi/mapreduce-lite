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

#include "src/base/class_register_test_helper.h"

CLASS_REGISTER_IMPLEMENT_REGISTRY(mapper_register, Mapper);
CLASS_REGISTER_IMPLEMENT_REGISTRY(second_mapper_register, Mapper);
CLASS_REGISTER_IMPLEMENT_REGISTRY(reducer_register, Reducer);
CLASS_REGISTER_IMPLEMENT_REGISTRY(file_impl_register, FileImpl);


class HelloMapper : public Mapper {
  virtual std::string GetMapperName() const {
    return "HelloMapper";
  }
};
REGISTER_MAPPER(HelloMapper);

class WorldMapper : public Mapper {
  virtual std::string GetMapperName() const {
    return "WorldMapper";
  }
};
REGISTER_MAPPER(WorldMapper);

class SecondaryMapper : public Mapper {
  virtual std::string GetMapperName() const {
    return "SecondaryMapper";
  }
};
REGISTER_SECONDARY_MAPPER(SecondaryMapper);


class HelloReducer : public Reducer {
  virtual std::string GetReducerName() const {
    return "HelloReducer";
  }
};
REGISTER_REDUCER(HelloReducer);

class WorldReducer : public Reducer {
  virtual std::string GetReducerName() const {
    return "WorldReducer";
  }
};
REGISTER_REDUCER(WorldReducer);


class LocalFileImpl : public FileImpl {
  virtual std::string GetFileImplName() const {
    return "LocalFileImpl";
  }
};
REGISTER_DEFAULT_FILE_IMPL(LocalFileImpl);
REGISTER_FILE_IMPL("/local", LocalFileImpl);

class MemFileImpl : public FileImpl {
  virtual std::string GetFileImplName() const {
    return "MemFileImpl";
  }
};
REGISTER_FILE_IMPL("/mem", MemFileImpl);

class NetworkFileImpl : public FileImpl {
  virtual std::string GetFileImplName() const {
    return "NetworkFileImpl";
  }
};
REGISTER_FILE_IMPL("/nfs", NetworkFileImpl);
