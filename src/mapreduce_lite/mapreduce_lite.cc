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
#include "src/mapreduce_lite/mapreduce_lite.h"

#include <stdio.h>

#include <map>
#include <new>
#include <set>
#include <string>
#include <vector>

#include "boost/filesystem.hpp"

#include "src/base/common.h"
#include "src/base/scoped_ptr.h"
#include "src/base/stl-util.h"
#include "gflags/gflags.h"
#include "src/hash/simple_hash.h"
#include "src/mapreduce_lite/socket_communicator.h"
#include "src/mapreduce_lite/flags.h"
#include "src/mapreduce_lite/protofile.h"
#include "src/mapreduce_lite/reader.h"
#include "google/protobuf/message.h"
#include "src/sorted_buffer/sorted_buffer.h"
#include "src/sorted_buffer/sorted_buffer.cc"
#include "src/strutil/join_strings.h"
#include "src/strutil/stringprintf.h"
#include "src/system/filepattern.h"


CLASS_REGISTER_IMPLEMENT_REGISTRY(mapreduce_lite_mapper_registry,
                                  mapreduce_lite::Mapper);
CLASS_REGISTER_IMPLEMENT_REGISTRY(mapreduce_lite_incremental_reducer_registry,
                                  mapreduce_lite::IncrementalReducer);
CLASS_REGISTER_IMPLEMENT_REGISTRY(mapreduce_lite_batch_reducer_registry,
                                  mapreduce_lite::BatchReducer);


namespace mapreduce_lite {

using sorted_buffer::SortedBuffer;
using sorted_buffer::SortedBufferIteratorImpl;
using std::map;
using std::string;
using std::vector;

//-----------------------------------------------------------------------------
// MapReduce context, using poor guy's singleton.
//-----------------------------------------------------------------------------

scoped_ptr<Mapper>& GetMapper() {
  static scoped_ptr<Mapper> mapper;
  return mapper;
}

scoped_ptr<ReducerBase>& GetReducer() {
  static scoped_ptr<ReducerBase> reducer;
  return reducer;
}

scoped_ptr<Communicator>& GetCommunicator() {
  static scoped_ptr<Communicator> communicator(new SocketCommunicator);
  return communicator;
}

scoped_ptr<vector<FILE*> >& GetOutputFileDescriptors() {
  static scoped_ptr<vector<FILE*> > output_files(new vector<FILE*>);
  return output_files;
}

scoped_ptr<string>& GetCurrentInputFilename() {
  static scoped_ptr<string> current_input_filename(new string);
  return current_input_filename;
}

scoped_array<char>& GetMapOutputSendBuffer() {
  static scoped_array<char> map_output_send_buffer;
  return map_output_send_buffer;
}

scoped_array<char>& GetMapOutputReceiveBuffer() {
  static scoped_array<char> map_output_receive_buffer;
  return map_output_receive_buffer;
}

scoped_ptr<vector<SortedBuffer*> >& GetReduceInputBuffers() {
  static scoped_ptr<vector<SortedBuffer*> > reduce_input_buffers(
      new vector<SortedBuffer*>);
  return reduce_input_buffers;
}

// Mapper::Output and Mapper::OutputToShard will increase
// this counter once per invocation.  Mapper::OutputToAllShards
// increases this counter by the number of reduce workers.
int g_count_map_output = 0;

//-----------------------------------------------------------------------------
// Initialiation and finalization of MapReduce Lite:
//-----------------------------------------------------------------------------

bool Initialize() {
  // Predicates like IAmMapWorker depends on ValidateCommandLineFlags().
  if (!ValidateCommandLineFlags()) {
    LOG(ERROR) << "Failed validating command line flags.";
    return false;
  }

  // Redirect log messages to disk files from terminal.  Note that
  // LogFilebase() is valid only after ValidAteCommandLineFlags.
  string filename_prefix = LogFilebase();
  InitializeLogger(StringPrintf("%s.INFO", filename_prefix.c_str()),
                   StringPrintf("%s.WARN", filename_prefix.c_str()),
                   StringPrintf("%s.ERROR", filename_prefix.c_str()));

  // Initialize the communicator.
  if (!FLAGS_mr_batch_reduction) {
    if (!GetCommunicator()->Initialize(IAmMapWorker(),
                                       NumMapWorkers(),
                                       ReduceWorkers(),
                                       MessageQueueSize(),
                                       MessageQueueSize(),
                                       WorkerId())) {
      LOG(ERROR) << "Cannot initialize communicator.";
      return false;
    }
  }

  // Create mapper instance.
  if (IAmMapWorker()) {
    GetMapper().reset(CreateMapper());
    if (GetMapper().get() == NULL) {
      return false;
    }
  }

  // Create reducer instance (if not map-only mode).
  if (IAmReduceWorker()) {
    GetReducer().reset(CreateReducer());
    if (GetReducer().get() == NULL) {
      return false;
    }
  }

  // Open output files.
  if (IAmReduceWorker() || IAmMapOnlyWorker()) {
    for (int i = 0; i < OutputFiles().size(); ++i) {
      FILE* file = fopen(OutputFiles()[i].c_str(), "w+");
      if (file == NULL) {
        LOG(ERROR) << "Cannot open output file: " << OutputFiles()[i];
        return false;
      }
      GetOutputFileDescriptors()->push_back(file);
    }
  }

  // Create map output sending buffer for map worker, if not in map-only mode,
  if (IAmMapWorker() && !IAmMapOnlyWorker()) {
    try {
      GetMapOutputSendBuffer().reset(new char[MapOutputBufferSize()]);
    } catch(std::bad_alloc&) {
      LOG(ERROR) << "Cannot allocation map output send buffer with size = "
                 << MapOutputBufferSize();
      return false;
    }
  }

  // Create map output receive buffer for reduce worker.
  if (IAmReduceWorker()) {
    try {
      GetMapOutputReceiveBuffer().reset(new char[MapOutputBufferSize()]);
    } catch(std::bad_alloc&) {
      LOG(ERROR) << "Cannot allocation map output receive buffer with size = "
                 << MapOutputBufferSize();
      return false;
    }
  }

  // Create reduce input buffer files for map worker, if in batch mode
  if (IAmMapWorker() && FLAGS_mr_batch_reduction) {
    GetReduceInputBuffers()->resize(NumReduceWorkers());
    try {
      for (int i = 0; i < NumReduceWorkers(); ++i) {
        (*GetReduceInputBuffers())[i] = new SortedBuffer(
            MapOutputBufferFilebase(i), ReduceInputBufferSize());
        LOG(INFO) << "create map output buffer"
                  << i
                  << MapOutputBufferFilebase(i);
      }
    } catch(const std::bad_alloc&) {
      LOG(FATAL) << "Insufficient memory for creating reduce input buffer.";
    }
  }

  return true;
}

void Finalize() {
  // For map workers, Communicator::Finalize() notifies all reduce
  // workers that this map worker has finished it work and will no
  // longer output anything.  For reduce workers,
  // Communicator::Finalize() releases binding and listening of TCP
  // sockets.
  if (!FLAGS_mr_batch_reduction) {
    GetCommunicator()->Finalize();
  }

  LOG(INFO) << "MapReduce Lite job finalized.";
}

//-----------------------------------------------------------------------------
// Implementation of reduce output facilities.
//
// Used by Mapper::Output* in map-only mode and ReducerBase::Output*.
//-----------------------------------------------------------------------------
void WriteText(FILE* output_stream, const string& key, const string& value) {
  fprintf(output_stream, "%s\n", value.c_str());
}

void ReduceOutput(int channel, const string& key, const string& value) {
  CHECK_LE(0, channel);
  CHECK_LT(channel, GetOutputFileDescriptors()->size());

  if (OutputFormat() == "text") {
    WriteText((*GetOutputFileDescriptors())[channel], key, value);
  } else if (OutputFormat() == "protofile") {
    protofile::WriteRecord((*GetOutputFileDescriptors())[channel], key, value);
  }
}

//-----------------------------------------------------------------------------
// Implementation of map output facilities.
//
// Used by Mapper::Output* if it is not in map-only mode.
//-----------------------------------------------------------------------------
void MapOutput(int reduce_worker_id, const string& key, const string& value) {
  // CHECK_LE(0, reduce_worker_id);
  CHECK_LT(reduce_worker_id, NumReduceWorkers());

  uint32* key_size = reinterpret_cast<uint32*>(GetMapOutputSendBuffer().get());
  uint32* value_size = key_size + 1;
  char* data = GetMapOutputSendBuffer().get() + 2 * sizeof(uint32);

  *key_size = key.size();
  *value_size = value.size();

  if (*key_size + *value_size + 2 * sizeof(uint32) > MapOutputBufferSize()) {
    LOG(FATAL) << "Too large map output, with key = " << key;
  }

  memcpy(data, key.data(), key.size());
  memcpy(data + *key_size, value.data(), value.size());

  if (!FLAGS_mr_batch_reduction) {
    if (reduce_worker_id >= 0) {
      if (GetCommunicator()->Send(GetMapOutputSendBuffer().get(),
                                  *key_size + *value_size + 2 * sizeof(uint32),
                                  reduce_worker_id) < 0) {
        LOG(FATAL) << "Send error to reduce worker: " << reduce_worker_id;
      }
    } else {
      for (int r_id = 0; r_id < NumReduceWorkers(); ++r_id) {
        if (GetCommunicator()->Send(
                GetMapOutputSendBuffer().get(),
                *key_size + *value_size + 2 * sizeof(uint32),
                r_id) < 0) {
          LOG(FATAL) << "Send error to reduce worker: " << r_id;
        }
      }
    }
  } else {
    if (reduce_worker_id >= 0) {
      (*GetReduceInputBuffers())[reduce_worker_id]->Insert(
          string(data, *key_size),
          string(data + *key_size, *value_size));
    } else {
      for (int r_id = 0; r_id < NumReduceWorkers(); ++r_id) {
        (*GetReduceInputBuffers())[r_id]->Insert(
            string(data, *key_size),
            string(data + *key_size, *value_size));
      }
    }
  }
}

//-----------------------------------------------------------------------------
// Implementation of ReducerBase:
//-----------------------------------------------------------------------------
void ReducerBase::Output(const string& key, const string& value) {
  ReduceOutput(0, key, value);
}

void ReducerBase::OutputToChannel(int channel,
                                  const string& key, const string& value) {
  ReduceOutput(channel, key, value);
}

const string& ReducerBase::GetOutputFormat() const {
  return OutputFormat();
}

int ReducerBase::NumOutputChannels() const {
  return GetOutputFileDescriptors()->size();
}

//-----------------------------------------------------------------------------
// Implementation of Mapper:
//-----------------------------------------------------------------------------
int Mapper::Shard(const string& key, int num_reduce_workers) {
  return JSHash(key) % num_reduce_workers;
}

void Mapper::Output(const string& key, const string& value) {
  if (IAmMapOnlyWorker()) {
    ReduceOutput(0, key, value);
  } else {
    MapOutput(Shard(key, NumReduceWorkers()), key, value);
  }
  ++g_count_map_output;
}

void Mapper::OutputToShard(int reduce_shard,
                           const string& key, const string& value) {
  if (IAmMapOnlyWorker()) {
    LOG(FATAL) << "Must not invoke OutputToShard in map-only mode.";
  } else {
    MapOutput(reduce_shard, key, value);
  }
  ++g_count_map_output;
}

void Mapper::OutputToAllShards(const string& key, const string& value) {
  if (IAmMapOnlyWorker()) {
    LOG(FATAL) << "Must not invoke OutputToAllShards in map-only mode.";
  } else {
    MapOutput(-1, key, value);
  }
  g_count_map_output += NumReduceWorkers();
}

const string& Mapper::CurrentInputFilename() const {
  return *GetCurrentInputFilename();
}

const string& Mapper::GetInputFormat() const {
  return InputFormat();
}

const string& Mapper::GetOutputFormat() const {
  if (!IsMapOnly()) {
    LOG(WARNING) << "Mapper shouldn't check output format if not map-only.";
  }
  return OutputFormat();
}

int Mapper::GetNumReduceShards() const {
  return NumReduceWorkers();
}

bool Mapper::IsMapOnly() const {
  CHECK(IAmMapWorker());
  return IAmMapOnlyWorker();
}

//-----------------------------------------------------------------------------
// Implementation of map worker:
//-----------------------------------------------------------------------------
void MapWork() {
  // Clear counters.
  g_count_map_output = 0;
  int count_map_input = 0;
  int count_input_shards = 0;

  FilepatternMatcher matcher(InputFilepattern());
  if (!matcher.NoError()) {
    LOG(FATAL) << "Failed matching: " << InputFilepattern();
  }

  for (int i_file = 0; i_file < matcher.NumMatched(); ++i_file) {
    *GetCurrentInputFilename() = matcher.Matched(i_file);
    LOG(INFO) << "Mapping input file: " << *GetCurrentInputFilename();

    scoped_ptr<Reader> reader(CREATE_READER(InputFormat()));
    if (reader.get() == NULL) {
      LOG(FATAL) << "Creating reader for: " << *GetCurrentInputFilename();
    }
    reader->Open(GetCurrentInputFilename()->c_str());

    GetMapper()->Start();

    string key, value;
    while (true) {
      if (!reader->Read(&key, &value)) {
        break;
      }

      GetMapper()->Map(key, value);
      ++count_map_input;
      if ((count_map_input % 1000) == 0) {
        LOG(INFO) << "Processed " << count_map_input << " records.";
      }
    }

    GetMapper()->Flush();
    ++count_input_shards;
    LOG(INFO) << "Finished mapping file: " << *GetCurrentInputFilename();
  }

  LOG(INFO) << "Map worker succeeded:\n"
            << " count_map_input = " << count_map_input << "\n"
            << " count_input_shards = " << count_input_shards << "\n"
            << " count_map_output = " << g_count_map_output;

  GetMapOutputSendBuffer().reset(NULL);

  // Flush all reduce_input_buffers
  if (!IAmMapOnlyWorker()) {
    STLDeleteElementsAndClear(GetReduceInputBuffers().get());
  }
}

//-----------------------------------------------------------------------------
// Implementation of reduce worker:
//-----------------------------------------------------------------------------
void ReduceWork() {
  LOG(INFO) << "Reduce worker in "
            << (FLAGS_mr_batch_reduction ? "batch " : "incremental ")
            << "reduction mode.";
  LOG(INFO) << "Output to " << JoinStrings(OutputFiles(), ",");

  // In order to implement the classical MapReduce API, which defines
  // reduce operation in a ``batch'' way -- reduce is invoked after
  // all reduce values were collected for a map output key.  we
  // employes Berkeley DB to sort and store map outputs arrived in
  // this reduce worker.  Berkeley DB is in response to keep a small
  // memory footprint and does external sort using disk.

  // MRML supports in addition ``incremental'' reduction, where
  // reduce() accepts an intermediate reduce result (represented by a
  // void*, and is NULL for the first value in a reduce input comes)
  // and a reduce value.  It should update the intermediate result
  // using the value.
  typedef map<string, void*> PartialReduceResults;
  scoped_ptr<PartialReduceResults> partial_reduce_results;

  // Initialize partial reduce results, or reduce input buffer.
  if (!FLAGS_mr_batch_reduction) {
    partial_reduce_results.reset(new PartialReduceResults);
  }

  // Loop over map outputs arrived in this reduce worker.
  LOG(INFO) << "Start receiving and processing arriving map outputs ...";
  int32 count_reduce = 0;
  int32 count_map_output = 0;
  int receive_status = 0;

  GetReducer()->Start();

  if (!FLAGS_mr_batch_reduction) {
    while ((receive_status =
            GetCommunicator()->Receive(GetMapOutputReceiveBuffer().get(),
                                       MapOutputBufferSize())) > 0) {
      ++count_map_output;
      uint32* p = reinterpret_cast<uint32*>(GetMapOutputReceiveBuffer().get());
      uint32 key_size = *p;
      uint32 value_size = *(p + 1);
      char* data = GetMapOutputReceiveBuffer().get() + sizeof(uint32) * 2;

      string key(data, key_size);
      string value(data + key_size, value_size);

      // Begin a new reduce, which insert a partial result, or does
      // partial reduce, which updates a partial result.
      PartialReduceResults::iterator iter = partial_reduce_results->find(key);
      if (iter == partial_reduce_results->end()) {
        (*partial_reduce_results)[key] =
            reinterpret_cast<IncrementalReducer*>(GetReducer().get())->
            BeginReduce(key, value);
      } else {
        reinterpret_cast<IncrementalReducer*>(GetReducer().get())->
            PartialReduce(key, value, iter->second);
      }

      if ((count_map_output % 5000) == 0) {
        LOG(INFO) << "Processed " << count_map_output << " map outputs.";
      }
    }

    if (receive_status < 0) {  // Check the reason of breaking while loop.
      LOG(FATAL) << "Communication error at reducer receiving.";
    }
  }

  GetMapOutputReceiveBuffer().reset(NULL);

  // Invoke EndReduce in incremental reduction mode, or invoke Reduce
  // in batch reduction mode.
  if (!FLAGS_mr_batch_reduction) {
    LOG(INFO) << "Finalizing incremental reduction ...";
    for (PartialReduceResults::const_iterator iter =
             partial_reduce_results->begin();
         iter != partial_reduce_results->end(); ++iter) {
      reinterpret_cast<IncrementalReducer*>(GetReducer().get())->
          EndReduce(iter->first, iter->second);
      // Note: the deletion of iter->second must be done by the user
      // program in EndReduce, because mrml.cc does not know the type of
      // ReducePartialResult defined by the user program.
      ++count_reduce;
    }
    LOG(INFO) << "Succeeded finalizing incremental reduction.";
  } else {
    LOG(INFO) << "Start batch reduction ...";
    LOG(INFO) << "Creating reduce input iterator ... filebase = "
              << ReduceInputBufferFilebase()
              << " with file num = "
              << NumReduceInputBufferFiles();
    SortedBufferIteratorImpl reduce_input_iterator(ReduceInputBufferFilebase(),
                                                   NumReduceInputBufferFiles());
    LOG(INFO) << "Succeeded creating reduce input iterator.";

    for (count_reduce = 0;
         !(reduce_input_iterator.FinishedAll());
         reduce_input_iterator.NextKey(), ++count_reduce) {
      reinterpret_cast<BatchReducer*>(GetReducer().get())->
          Reduce(reduce_input_iterator.key(), &reduce_input_iterator);
      if (count_reduce > 0 && (count_reduce % 5000) == 0) {
        LOG(INFO) << "Invoked " << count_reduce << " reduce()s.";
      }
    }

    // remove reduce input buffer files
    for (int i_file = 0; i_file < NumReduceInputBufferFiles(); ++i_file) {
      string filename = SortedBuffer::SortedFilename(
          ReduceInputBufferFilebase(), i_file);
      LOG(INFO) << "Removing : " << filename
                << " size = "<< boost::filesystem::file_size(filename);
      boost::filesystem::remove(filename);
    }

    LOG(INFO) << "Finished batch reduction.";
  }

  GetReducer()->Flush();

  LOG(INFO) << " count_reduce = " << count_reduce << "\n"
            << " count_map_output = " << count_map_output << "\n";
}

}  // namespace mapreduce_lite
