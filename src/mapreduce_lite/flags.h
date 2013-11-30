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
// Here declares flags used by map/reduce workers to accept instructions from
// MapReduce Lite scheduler.  For desciptions on these flags, please refer to
// flags.cc.  For more details, refer to design.txt.
//
#ifndef MAPREDUCE_LITE_FLAGS_H_
#define MAPREDUCE_LITE_FLAGS_H_

#include <string>
#include <vector>

#include "gflags/gflags.h"
#include "src/mapreduce_lite/mapreduce_lite.h"

//-----------------------------------------------------------------------------
// Most flags should be accessed using the following accessors.
//-----------------------------------------------------------------------------
DECLARE_string(mr_mapper_class);
DECLARE_string(mr_reducer_class);
DECLARE_string(mr_input_format);
DECLARE_string(mr_output_format);
DECLARE_bool(mr_batch_reduction);
DECLARE_int32(mr_max_map_output_size);
namespace mapreduce_lite {

//-----------------------------------------------------------------------------
// Check the correctness of flags.
//-----------------------------------------------------------------------------
bool ValidateCommandLineFlags();

//-----------------------------------------------------------------------------
// Invoke ValidateCommandLineFlags() before using the following accessors.
//-----------------------------------------------------------------------------
bool IAmMapWorker();
bool IAmReduceWorker();
bool IAmMapOnlyWorker();
const char* WorkerType();
int MapWorkerId();
int ReduceWorkerId();
int WorkerId();
int NumMapWorkers();
int NumReduceWorkers();
int NumWorkers();
int MessageQueueSize();
int NumReduceInputBufferFiles();
const std::string& InputFormat();
const std::string& OutputFormat();
const std::vector<std::string>& ReduceWorkers();
const std::string& InputFilepattern();
const std::vector<std::string>& OutputFiles();
std::string MapOutputBufferFilebase(int reducer_id);
std::string ReduceInputBufferFilebase();
int ReduceInputBufferSize();
int MapOutputBufferSize();
std::string LogFilebase();
Mapper* CreateMapper();
ReducerBase* CreateReducer();

}  // namespace mapreduce_lite

#endif  // MAPREDUCE_LITE_FLAGS_H_
