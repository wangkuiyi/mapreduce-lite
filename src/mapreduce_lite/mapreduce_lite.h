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
// Define the programming API of MapReduce Lite.
//
#ifndef MAPREDUCE_LITE_MAPREDUCE_LITE_H_
#define MAPREDUCE_LITE_MAPREDUCE_LITE_H_

#include <string>
#include <sstream>
#include <vector>

#include "src/base/class_register.h"
#include "src/sorted_buffer/sorted_buffer.h"

namespace google {
namespace protobuf {
class Message;
}
}

namespace mapreduce_lite {

typedef ::google::protobuf::Message ProtoMessage;
typedef ::sorted_buffer::SortedBufferIterator ReduceInputIterator;

using std::string;

//-----------------------------------------------------------------------------
//
// Mapper class
//
// *** Basic API ***
//
// In MapReduce Lite, for each map input shard file, a map worker (OS process)
// is created.  The worker creates an object of a derived-class of
// Mapper, then invokes its member functions in the following
// procedure:
//
//  1. Before processing the first key-value pair in the map input
//     shard, it invokes Start().  You may want to override Start() to
//     do shard-specific initializations.
//  2. For each key-value pair in the shard, it invokes Map().
//  3. After all map input pairs were processed, it invokes Flush().
//
// *** Multi-pass Map ***
//
// A unique feature of MapReduce Lite (differs from Google MapReduce
// and Hadoop) is that above procedure may be repeated for multiple
// time, if the command line paramter --mrml_multipass_map is set with
// a positive value.  In this case, derived classes can invoke
// GetCurrentPass() to get the current (zero-based) pass id.
//
// *** Sharding ***
//
// As both Google MapReduce API and Hadoop API, programmers can
// specify to where a map output goes by overriding Shard().  In
// addition, similar to Google API (but differs from Hadoop API),
// programmers can also invoke OutputToShard() with a parameter
// specifying the target reduce shard.
//
// *** Output to All Shards ***
//
// A unique feature of MapReduce Lite is OutputToAllShards(), which
// allows a map output goes to all shards.  Some machine learning
// algorithms (e.g., AD-LDA) might find this API useful.
//
// *** NOTE ***
//
// OutputToShard and OutputToAllShards are forbidden in map-only mode.
//
//-----------------------------------------------------------------------------
class Mapper {
 public:
  virtual ~Mapper() {}

  virtual void Start() {}
  virtual void Map(const string& key, const string& value) = 0;
  virtual void Flush() {}
  virtual int Shard(const string& key, int num_reduce_shards);

 protected:
  virtual void Output(const string& key, const string& value);
  virtual void OutputToShard(int reduce_shard,
                             const string& key, const string& value);
  virtual void OutputToAllShards(const string& key, const string& value);

  const string& CurrentInputFilename() const;
  const string& GetInputFormat() const;
  const string& GetOutputFormat() const;
  int GetNumReduceShards() const;
  bool IsMapOnly() const;
};

//-----------------------------------------------------------------------------
//
// IncrementalReducer class (providing a MapReduce Lite-specific API)
//
// *** Combiner ***
//
// IncrementalReducer has two member functions: Start() and Flush():
//
//  1. Before processing a reduce shard, invokes Start();
//  2. For each reduce input (consisting a key and one or more
//     values), invokes BeginReduce(), PartialReduce(), and
//     EndReduce().
//  3. After processing a reduce shard, invokes Flush();
//
// This is similar to Mapper, and allows the implementation of
// combiner pattern in reduce class.
//
// *** Incremental Reduce ***
//
// In standard MapReduce API, thre is a Reduce() function which takes
// all values associated with a key, and programmers override this
// function to process all these values as a whole.  However, in
// practice (both Google MapReduce and Hadoop), the access to these
// values are constrained to be through a forward-only iterator.  This
// constraint, in logic, is equivalent to _incremental reduce_.  In
// MapReduce Lite, we represent incremental reduce by three member functions:
//
//  1. void* BeginReduce(key, value): Given the first value in a
//     reduce input, returns a pointer to intermediate reducing
//     result.
//
//  2. void PartialReduce(key, value, partial_result): For each of the
//     rest values (except for the first value) in current reduce
//     input, update intermediate reducing result.
//
//  3. void EndReduce(partial_result, output): When this function is
//     invoked, partial_result points the _final_ result.  This
//     function should output the final result into a string, which,
//     together with the key of the current reduce input, will be save
//     as a reduce output pair.
//
//-----------------------------------------------------------------------------
class ReducerBase {
 public:
  virtual ~ReducerBase() {}
  const string& GetOutputFormat() const;

  virtual void Start() {}
  virtual void Flush() {}

  // Output to the first output channel (the first output file).
  virtual void Output(const string& key,
                      const string& value);

  // Output to the specified output channel.
  virtual void OutputToChannel(int channel,
                               const string& key,
                               const string& value);

  // The number of output channels. The parameter channel in Output*
  // must be in the range [0, NumOutputChannels() - 1].
  virtual int NumOutputChannels() const;
};


class IncrementalReducer : public ReducerBase {
 public:
  virtual ~IncrementalReducer() {}

  virtual void* BeginReduce(const string& key,
                            const string& value) = 0;
  virtual void PartialReduce(const string& key,
                             const string& value,
                             void* partial_result) = 0;
  virtual void EndReduce(const string& key,
                         void* partial_result) = 0;
};

//-----------------------------------------------------------------------------
//
// In addition to the IncrementalReducer API, MapReduce Lite has BatchReducer
// which provides the same API as Google MapReduce and is suitable for
// processing large scale data; in particular, without the limitation on the
// number of unique map output keys.
//
// *** Batch Reduction ***
//
// The initial design of MRML is to provide a way which makes it
// possible for map workers and reduce workers work simultaneously.
// The design was realized by MRML_Reducer class.  However, this
// design has a limit on the number of unique map output keys, which
// would becomes a servere problem in applications like parallel
// training of language models.  Therefore, we add MR_Redcuer, a
// reducer API which is identical to that published in Google papers.
//
// If you derive your reducer class from MR_Reducer, instead of
// MRML_Reducer, please remember to register it using
// REGISTER_MR_REDUCER instead of REGISTER_REDUCER.
//
//-----------------------------------------------------------------------------
class BatchReducer : public ReducerBase {
 public:
  virtual ~BatchReducer() {}

  virtual void Reduce(const string& key,
                      ReduceInputIterator* values) = 0;
};

}  // namespace mapreduce_lite


//-----------------------------------------------------------------------------
// REGISTER_MAPPER, REGISTER_INCREMENTAL_REDUCER and REGISTER_BATCH_REDUCER.
//
// MapReduce Lite mapper/reducer registering mechanism.  Each
// user-defined mapper, say UserDefinedMapper, must be registerred
// using REGISTER_MAPPER(UserDefinedMapper); in a .cc file.
// Similarly, Each user-defined reducer must be registered using
// REGISTER_INCREMENTAL_REDUCER() or REGISTER_BATCH_REDUCER; This
// allows the MapReduce Lite runtime to create instances of
// mappers/reducers according to their names given as command line
// parameters.
//-----------------------------------------------------------------------------
CLASS_REGISTER_DEFINE_REGISTRY(mapreduce_lite_mapper_registry,
                               mapreduce_lite::Mapper);
CLASS_REGISTER_DEFINE_REGISTRY(mapreduce_lite_incremental_reducer_registry,
                               mapreduce_lite::IncrementalReducer);
CLASS_REGISTER_DEFINE_REGISTRY(mapreduce_lite_batch_reducer_registry,
                               mapreduce_lite::BatchReducer);

#define REGISTER_MAPPER(mapper_name)            \
  CLASS_REGISTER_OBJECT_CREATOR(                \
      mapreduce_lite_mapper_registry,           \
      mapreduce_lite::Mapper,                   \
      #mapper_name,                             \
      mapper_name)

#define CREATE_MAPPER(mapper_name_as_string)    \
  CLASS_REGISTER_CREATE_OBJECT(                 \
      mapreduce_lite_mapper_registry,           \
      mapper_name_as_string)

#define REGISTER_INCREMENTAL_REDUCER(incremental_reducer_name)  \
  CLASS_REGISTER_OBJECT_CREATOR(                                \
      mapreduce_lite_incremental_reducer_registry,              \
      mapreduce_lite::IncrementalReducer,                       \
      #incremental_reducer_name,                                \
      incremental_reducer_name)

#define CREATE_INCREMENTAL_REDUCER(incremental_reducer_name_as_string)  \
  CLASS_REGISTER_CREATE_OBJECT(                                         \
      mapreduce_lite_incremental_reducer_registry,                      \
      incremental_reducer_name_as_string)

#define REGISTER_BATCH_REDUCER(batch_reducer_name)      \
  CLASS_REGISTER_OBJECT_CREATOR(                        \
      mapreduce_lite_batch_reducer_registry,            \
      mapreduce_lite::BatchReducer,                     \
      #batch_reducer_name,                              \
      batch_reducer_name)

#define CREATE_BATCH_REDUCER(batch_reducer_name_as_string)      \
  CLASS_REGISTER_CREATE_OBJECT(                                 \
      mapreduce_lite_batch_reducer_registry,                    \
      batch_reducer_name_as_string)

#endif  // MAPREDUCE_LITE_MAPREDUCE_LITE_H_
