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
// Copyright 2010 Tencent Inc.
// Author: Yi Wang (yiwang@tencent.com)
//
// The registeration of mapper and reducer classes has been tested in
// /base/class_register_test.cc.  Here we test whether we can define
// mapper / reducer classes in namespace and create them from outside
// of the namespace.

#include <string>

#include "gtest/gtest.h"
#include "src/mapreduce_lite/mapreduce_lite.h"

using mapreduce_lite::Mapper;
using mapreduce_lite::IncrementalReducer;
using mapreduce_lite::BatchReducer;
using mapreduce_lite::ReduceInputIterator;
using std::string;


namespace a_test_namespace {

// A maper class, an incremental reducer class and a batch reducer class:
class ATestMapper : public mapreduce_lite::Mapper {
 public:
  virtual void Map(const string& key, const string& value) {}
  const char* name() { return "ATestMapper"; }
};

class ATestIncrementalReducer : public mapreduce_lite::IncrementalReducer {
 public:
  virtual void* BeginReduce(const string& key, const string& value) {
    return NULL;
  }
  virtual void PartialReduce(const string& key, const string& value,
                             void* partial_result) {}
  virtual void EndReduce(const string& key, void* partial_result) {}
  const char* name() { return "ATestIncrementalReducer"; }
};

class ATestBatchReducer : public mapreduce_lite::BatchReducer {
 public:
  virtual void Reduce(const string& key, ReduceInputIterator* values) {}
  const char* name() { return "ATestBatchReducer"; }
};

REGISTER_MAPPER(ATestMapper);
REGISTER_INCREMENTAL_REDUCER(ATestIncrementalReducer);
REGISTER_BATCH_REDUCER(ATestBatchReducer);

}  // namespace a_test_namespace


TEST(MapReduceLiteRegistererTest, CreateMapperReducer) {
  Mapper* mapper = CREATE_MAPPER("ATestMapper");
  EXPECT_TRUE(mapper != NULL);

  BatchReducer* batch_reducer =
      CREATE_BATCH_REDUCER("ATestBatchReducer");
  EXPECT_TRUE(batch_reducer != NULL);

  IncrementalReducer* incremental_reducer =
      CREATE_INCREMENTAL_REDUCER("ATestIncrementalReducer");
  EXPECT_TRUE(incremental_reducer != NULL);
}
