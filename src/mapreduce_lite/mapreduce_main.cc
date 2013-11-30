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
#include "src/base/common.h"
#include "gflags/gflags.h"
#include "src/mapreduce_lite/flags.h"
#include "src/mapreduce_lite/mapreduce_lite.h"

namespace mapreduce_lite {
bool IAmMapWorker();
bool Initialize();
void Finalize();
void MapWork();
void ReduceWork();
}  // namespace mapreduce_lite

//-----------------------------------------------------------------------------
// The pre-defined main function
//-----------------------------------------------------------------------------

int main(int argc, char** argv) {
  // Parse command line flags, leaving argc unchanged, but rearrange
  // the arguments in argv so that the flags are all at the beginning.
  google::ParseCommandLineFlags(&argc, &argv, false);

  if (!mapreduce_lite::Initialize()) {
    LOG(ERROR) << "Initialization of MapReduce Lite failed.";
    return -1;
  }

  LOG(INFO) << "I am a " << (mapreduce_lite::IAmMapWorker() ? "map worker" :
                             "reduce worker");

  if (mapreduce_lite::IAmMapWorker()) {
    mapreduce_lite::MapWork();
  } else {
    mapreduce_lite::ReduceWork();
  }

  mapreduce_lite::Finalize();
  return 0;
}
