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
// Define flags used by map/reduce workers to accept instructions from
// MapReduce Lite scheduler.
//
#include "src/mapreduce_lite/flags.h"

#include <sys/utsname.h>                // For uname
#include <time.h>
#include <unistd.h>

#include <string>
#include <vector>

#include "boost/filesystem.hpp"

#include "src/base/common.h"
#include "src/base/scoped_ptr.h"
#include "gflags/gflags.h"
#include "src/mapreduce_lite/mapreduce_lite.h"
#include "src/sorted_buffer/sorted_buffer.h"
#include "src/strutil/stringprintf.h"
#include "src/strutil/split_string.h"


namespace mapreduce_lite {
const int kDefaultReduceInputBufferSize = 256;       // 256 MB
const int kDefaultMapperMessageQueueSize = 32;       // 32 MB
const int kDefaultReducerMessageQueueSize = 128;     // 128 MB
const int kDefaultMapOutputSize = 32 * 1024 * 1024;  // 32 MB
}  // namespace mapreduce_lite


DEFINE_int32(mr_num_map_workers, 0,
             "The number of map workers, required by reduce workers to "
             "check whether all map workers had finished their work.  This "
             "flag is set only for reduce workers.");

DEFINE_string(mr_reduce_workers, "",
              "A set of reduce workers, each identified by \"hostname:port\". "
              "This flag is set only for map workers, as each map worker need "
              "to set up network connections to all reduce workers.");

DEFINE_int32(mr_map_worker_id, -1,
             "A zero-based integer index denoting the map worker id."
             "This flag is set only for map workers to let them know who they "
             "are.");

DEFINE_int32(mr_reduce_worker_id, -1,
             "A zero-based integer index in the range of mr_reduce_workers "
             "denoting the reduce worker id.  This flag is set only for "
             "reduce workers to let them know who they are.");

DEFINE_bool(mr_map_only, false,
            "Denote whether this MapReduce job is running in map-only mode.");

DEFINE_bool(mr_batch_reduction, true,
            "By default, MapReduce Lite works in batch reduction mode, the "
            "one compatible with Google MapReduce and Hadoop.  Or, it can "
            "work in the incremental reduction mode, which is a specific "
            "function provided by MapReduce Lite for fast reduction.");

DEFINE_string(mr_mapper_class, "",
              "This flag is set only for map workers to let them know the "
              "mapper class that they should execute.  Note that this flag "
              "implies that each map worker can execute only one mapper "
              "class in its life.  This might be changed in the future when we"
              "support better work load balance.");

DEFINE_string(mr_reducer_class, "",
              "This flag is set only for reduce workers to let them know the "
              "reducer class that they should execute.");

DEFINE_string(mr_input_filepattern, "",
              "A set of comma separated input files which will be processed "
              "by one map worker using one mapper class.  This flag is set "
              "only for map workers.");

DEFINE_string(mr_output_files, "",
              "A map-only worker or a reduce worker may generate one or more "
              "output files, each for a reduce output channel.  Usually there "
              "is only one output channel, thus one output file per worker.");

DEFINE_string(mr_input_format, "text",
              "The input format, can be either \"text\", \"recordio\", or "
              "\"protofile\".  This flag is set only for map workers.");

DEFINE_string(mr_output_format, "text",
              "The output format, can be either \"text\", \"recordio\", or "
              "\"protofile\".  This flag is set only for reduce workers.");

DEFINE_string(mr_reduce_input_filebase, "",
              "In MapReduce Lite, reducer workers recieve and sort map inputs "
              "using disk files.  Each reduce worker may generate one or more "
              "disk files, which share the same filebase.");

DEFINE_int32(mr_reduce_input_buffer_size,
             mapreduce_lite::kDefaultReduceInputBufferSize,
             "The size of each reduce input buffer swap file in mega-bytes.");

DEFINE_int32(mr_num_reduce_input_buffer_files, -1,
             "This number will be passed to a reduce worker to tell the "
             "number of input files to it, after the scheduler copied map "
             "output files, a.k.a., reduce input buffer files, into reduce "
             "machines.");

DEFINE_string(mr_log_filebase, "",
              "The real log filename is mr_log_filebase appended by worker "
              "type, worker id, date, time, process_id, log type and etc");

DEFINE_int32(mr_mapper_message_queue_size,
             mapreduce_lite::kDefaultMapperMessageQueueSize,
             "The map worker maintains N message queues, each for a reduce "
             "worker.  These queues are buffers between the producer/consumer "
             "communication model. This flag is in mega-bytes.");

DEFINE_int32(mr_reducer_message_queue_size,
             mapreduce_lite::kDefaultReducerMessageQueueSize,
             "A reduce worker maintains a message queue, as buffers between "
             "the producer/consumer communication model.  This flag is in "
             "mega-bytes");

DEFINE_int32(mr_max_map_output_size,
             mapreduce_lite::kDefaultMapOutputSize,
             "The max size of a map output, in bytes.");

namespace mapreduce_lite {

//-----------------------------------------------------------------------------
// Poor guy's singletons:
//-----------------------------------------------------------------------------
typedef std::vector<std::string> StringVector;

static scoped_ptr<StringVector>& GetReduceWorkers() {
  static scoped_ptr<StringVector> reduce_workers(new StringVector);
  return reduce_workers;
}

static scoped_ptr<StringVector>& GetOutputFiles() {
  static scoped_ptr<StringVector> output_files(new StringVector);
  return output_files;
}

//-----------------------------------------------------------------------------
// Check the correctness of flags.
//-----------------------------------------------------------------------------

bool ValidateCommandLineFlags() {
  bool flags_valid = true;

  // Check positve number of map workers. Validates NumMapWorkers().
  if (FLAGS_mr_num_map_workers <= 0) {
    LOG(ERROR) << "mr_num_map_workers must be a positive value.";
    flags_valid = false;
  }

  // Check positive number of reduce workers. Validates NumReduceWorkers().
  SplitStringUsing(FLAGS_mr_reduce_workers, ",", GetReduceWorkers().get());
  if (GetReduceWorkers()->size() == 0 && !FLAGS_mr_map_only) {
    LOG(ERROR) << "As is not in map-only mode, mr_reduce_workers must specify "
               << "one or more reduce workers.";
    flags_valid = false;
  }
  if (GetReduceWorkers()->size() > 0 && FLAGS_mr_map_only) {
    LOG(ERROR) << "As is in map-only mode, mr_reduce_workers must be empty.";
    flags_valid = false;
  }

  // In map-only mode, only map_worker_id, but not reduce_worker_id, is set.
  // If not in map-only mode, either map_worker_id or reduce_worker_id is set.
  // This check makes sure that IAmMapWorker() can predicate the worker role.
  // Validates IAmMapWorker() and IAmReduceWorker().
  if (FLAGS_mr_map_only &&
      (FLAGS_mr_map_worker_id < 0 || FLAGS_mr_reduce_worker_id >= 0)) {
    LOG(ERROR) << "In map only mode, mr_map_worker_id must be set and "
               << "mr_reduce_worker_id must not be set.";
    flags_valid = false;
  }
  if (!FLAGS_mr_map_only &&
      FLAGS_mr_map_worker_id >=0 && FLAGS_mr_reduce_worker_id >= 0) {
    LOG(ERROR) << "Both mr_map_worker_id and mr_reduce_worker_id were set, "
               << "so there is no way to distinguish the role of a worker.";
    flags_valid = false;
  }

  // Check FLAGS_mr_map_worker_id in range [0, FLAGS_mr_num_map_workers - 1].
  if (IAmMapWorker() && FLAGS_mr_map_worker_id >= NumMapWorkers()) {
    LOG(ERROR) << "mr_map_worker_id (" << FLAGS_mr_map_worker_id << ") >= "
               << "mr_num_map_workers (" << FLAGS_mr_num_map_workers << ").";
    flags_valid = false;
  }

  // Checks FLAGS_mr_reduce_worker_id in range [0, num_reduce_workers - 1].
  if (IAmReduceWorker() && FLAGS_mr_reduce_worker_id >= NumReduceWorkers()) {
    LOG(ERROR) << "mr_reduce_worker_id (" << FLAGS_mr_reduce_worker_id
               << ") >= the number of IP:ports in mr_reduce_workers";
    flags_valid = false;
  }

  // Mapper class must be specified for a map worker
  if (IAmMapWorker() && FLAGS_mr_mapper_class.empty()) {
    LOG(ERROR) << "mr_mapper_class not specified.";
    flags_valid = false;
  }

  // Reducer class must be specified for a reducer worker.
  if (IAmReduceWorker() && FLAGS_mr_reducer_class.empty()) {
    LOG(ERROR) << "reducer_class not specified.";
    flags_valid = false;
  }

  // In batch reduction mode (but not map-only), validate reduce_input_filebase
  // and reduce_input_buffer_size.
  if (FLAGS_mr_batch_reduction && !FLAGS_mr_map_only) {
    if (FLAGS_mr_reduce_input_filebase.empty()) {
      LOG(ERROR) << "Please set mr_reduce_input_filebase in batch "
                 << "reduction mode (if not in map-only mode)";
      flags_valid = false;
    } else if (IAmMapWorker() && boost::filesystem::exists(
        ::sorted_buffer::SortedBuffer::SortedFilename(
            MapOutputBufferFilebase(0), 0))) {
      LOG(ERROR) << "Please delete existing reduce input buffer files: "
                 << MapOutputBufferFilebase(0) << "* ";
      flags_valid = false;
    } else if (FLAGS_mr_reduce_input_buffer_size < 1 ||
               FLAGS_mr_reduce_input_buffer_size > 2 * 1024) {
      LOG(ERROR) << "mr_reduce_input_buffer_size must be in [1MB, 2000MB]";
      flags_valid = false;
    }
  }

  // If input file format is unknown, set it to text.
  if (FLAGS_mr_input_format != "text" &&
      FLAGS_mr_input_format != "recordio" &&
      FLAGS_mr_input_format != "protofile") {
    LOG(ERROR) << "Unknown input_format: " << FLAGS_mr_input_format;
    flags_valid = false;
  }

  // If output file format is unknown, set it to text.
  if (FLAGS_mr_output_format != "text" &&
      FLAGS_mr_output_format != "recordio" &&
      FLAGS_mr_output_format != "protofile") {
    LOG(ERROR) << "Unknown output_format: " << FLAGS_mr_output_format;
    flags_valid = false;
  }

  // Map worker must has inputs specified.
  if (IAmMapWorker()) {
    if (FLAGS_mr_input_filepattern.empty()) {
      LOG(ERROR) << "For a map worker, mr_input_filepattern must be "
                 << "shell filepattern.";
      flags_valid = false;
    }
  }

  // Reduce worker or map-only map worker must have output specified.
  if (!IAmMapWorker() ||                        // as a reduce worker, or
      (IAmMapWorker() && FLAGS_mr_map_only)) {  // a map-only map worker
    SplitStringUsing(FLAGS_mr_output_files, ",", GetOutputFiles().get());
    if (GetOutputFiles()->size() <= 0) {
      LOG(ERROR) << "For a reduce worker (or a map-only map worker), "
                 << "mr_output_files must be comma-separated filenames.";
      flags_valid = false;
    }
  }

  // Check FLAGS_mr_mapper/reducer_message_queue_size are positive values.
  if (FLAGS_mr_mapper_message_queue_size <= 0 ||
      FLAGS_mr_reducer_message_queue_size <= 0) {
    LOG(ERROR) << "mr_mapper/reducer_message_queue_size <= 0.";
    flags_valid = false;
  }

  // Check positive mr_max_map_output_size
  if (FLAGS_mr_max_map_output_size <= 0) {
    LOG(ERROR) << "mr_max_map_output_size must be positive.";
    flags_valid = false;
  }

  return flags_valid;
}

//-----------------------------------------------------------------------------
// Flag accessors.
//-----------------------------------------------------------------------------

bool IAmMapWorker() {
  return FLAGS_mr_map_worker_id >= 0;
}

bool IAmReduceWorker() {
  return !IAmMapWorker();
}

bool IAmMapOnlyWorker() {
  return IAmMapWorker() && FLAGS_mr_map_only;
}

const char* WorkerType() {
  return IAmMapWorker() ? "mapper" : "reducer";
}

int MapWorkerId() {
  return FLAGS_mr_map_worker_id;
}

int ReduceWorkerId() {
  return FLAGS_mr_reduce_worker_id;
}

int WorkerId() {
  return IAmMapWorker() ? MapWorkerId() : ReduceWorkerId();
}

int NumMapWorkers() {
  return FLAGS_mr_num_map_workers;
}

int NumReduceWorkers() {
  return ReduceWorkers().size();
}

int NumWorkers() {
  return IAmMapWorker() ? NumMapWorkers() : NumReduceWorkers();
}

int MessageQueueSize() {
  return IAmMapWorker() ?
      FLAGS_mr_mapper_message_queue_size * 1024 * 1024 :
      FLAGS_mr_reducer_message_queue_size * 1024 * 1024;
}

const std::string& InputFormat() {
  return FLAGS_mr_input_format;
}

const std::string& OutputFormat() {
  return FLAGS_mr_output_format;
}

const std::vector<std::string>& ReduceWorkers() {
  return *GetReduceWorkers();
}

const std::string& InputFilepattern() {
  return FLAGS_mr_input_filepattern;
}

const std::vector<std::string>& OutputFiles() {
  return *GetOutputFiles();
}

std::string MapOutputBufferFilebase(int reducer_id) {
  // For map worker, to distinguish reduce input buffer files for different
  // reducer workers, mapper-id and reducer-id are appended to filebase.
  CHECK_LE(0, reducer_id);
  CHECK_GT(NumReduceWorkers(), reducer_id);
  return StringPrintf("%s-mapper-%05d-reducer-%05d",
                      FLAGS_mr_reduce_input_filebase.c_str(),
                      MapWorkerId(), reducer_id);
}

std::string ReduceInputBufferFilebase() {
  return FLAGS_mr_reduce_input_filebase;
}

int ReduceInputBufferSize() {
  // Converts MB to bytes.
  return FLAGS_mr_reduce_input_buffer_size * 1024 * 1024;
}

int NumReduceInputBufferFiles() {
  return FLAGS_mr_num_reduce_input_buffer_files;
}

int MapOutputBufferSize() {
  // The buffer holds the map output data and two uint32 values of key and
  // value size.
  return FLAGS_mr_max_map_output_size + 2 * sizeof(uint32);
}

std::string GetHostName() {
  struct utsname buf;
  if (0 != uname(&buf)) {
    *buf.nodename = '\0';
  }
  return std::string(buf.nodename);
}

std::string GetUserName() {
  const char* username = getenv("USER");
  return username != NULL ? username : getenv("USERNAME");
}

std::string PrintCurrentTime() {
  time_t current_time = time(NULL);
  struct tm broken_down_time;
  CHECK(localtime_r(&current_time, &broken_down_time) == &broken_down_time);
  return StringPrintf("%04d%02d%02d-%02d%02d%02d",
                      1900 + broken_down_time.tm_year,
                      1 + broken_down_time.tm_mon,
                      broken_down_time.tm_mday, broken_down_time.tm_hour,
                      broken_down_time.tm_min,  broken_down_time.tm_sec);
}

std::string LogFilebase() {
  // log_filebase := FLAGS_mr_log_filebase +
  //                 worker_type + worker_index + total_worker_num
  //                 node_name + username +
  //                 date_time + process_id
  CHECK(!FLAGS_mr_log_filebase.empty());
  std::string filename_prefix;
  SStringPrintf(&filename_prefix,
                "%s-%s-%05d-of-%05d.%s.%s.%s.%u",
                FLAGS_mr_log_filebase.c_str(),
                WorkerType(), WorkerId(), NumWorkers(),
                GetHostName().c_str(),
                GetUserName().c_str(),
                PrintCurrentTime().c_str(),
                getpid());
  return filename_prefix;
}

Mapper* CreateMapper() {
  Mapper* mapper = NULL;
  if (IAmMapWorker()) {
    mapper = CREATE_MAPPER(FLAGS_mr_mapper_class);
    if (mapper == NULL) {
      LOG(ERROR) << "Cannot create mapper: " << FLAGS_mr_mapper_class;
    }
  }
  return mapper;
}

ReducerBase* CreateReducer() {
  ReducerBase* reducer = NULL;
  if (IAmReduceWorker()) {
    reducer = (FLAGS_mr_batch_reduction ?
               reinterpret_cast<ReducerBase*>(
                   CREATE_BATCH_REDUCER(FLAGS_mr_reducer_class)) :
               reinterpret_cast<ReducerBase*>(
                   CREATE_INCREMENTAL_REDUCER(FLAGS_mr_reducer_class)));
    if (reducer == NULL) {
      LOG(ERROR) << "Cannot create reducer: " << FLAGS_mr_reducer_class;
    }
  }
  return reducer;
}

}  // namespace mapreduce_lite
