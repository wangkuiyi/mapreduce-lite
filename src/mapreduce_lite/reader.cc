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

#include "src/mapreduce_lite/reader.h"

#include <stdio.h>
#include <string.h>

#include "src/base/common.h"
#include "src/mapreduce_lite/protofile.h"
#include "src/mapreduce_lite/utils.h"
#include "src/strutil/stringprintf.h"

const int kDefaultMaxInputLineLength = 16 * 1024;    // 16 KB

DEFINE_int32(mr_max_input_line_length,
             kDefaultMaxInputLineLength,
             "The max line length if input is in text format.");


namespace mapreduce_lite {

CLASS_REGISTER_IMPLEMENT_REGISTRY(mapreduce_lite_reader_registry, Reader);
REGISTER_READER("text", TextReader);
REGISTER_READER("protofile", ProtoRecordReader);

//-----------------------------------------------------------------------------
// Implementation of Reader
//-----------------------------------------------------------------------------
void Reader::Open(const std::string& source_name) {
  Close();  // Ensure to close pre-opened file.
  input_filename_ = source_name;
  input_stream_ = OpenFileOrDie(source_name.c_str(), "r");
}

void Reader::Close() {
  if (input_stream_ != NULL) {
    fclose(input_stream_);
    input_stream_ = NULL;
  }
}

//-----------------------------------------------------------------------------
// Implementation of TextReader
//-----------------------------------------------------------------------------
TextReader::TextReader()
    : line_num_(0),
      reading_a_long_line_(false) {
  try {
    CHECK_LT(1, FLAGS_mr_max_input_line_length);
    line_.reset(new char[FLAGS_mr_max_input_line_length]);
  } catch(std::bad_alloc&) {
    LOG(FATAL) << "Cannot allocate line input buffer.";
  }
}

bool TextReader::Read(std::string* key, std::string* value) {
  if (input_stream_ == NULL) {
    return false;
  }

  SStringPrintf(key, "%s-%010lld",
                input_filename_.c_str(), ftell(input_stream_));
  value->clear();

  if (fgets(line_.get(), FLAGS_mr_max_input_line_length, input_stream_)
      == NULL) {
    return false;  // Either ferror or feof. Anyway, returns false to
                   // notify the caller no further reading operations.
  }

  int read_size = strlen(line_.get());
  if (line_[read_size - 1] != '\n') {
    LOG(ERROR) << "Encountered a too-long line (line_num = " << line_num_
               << ").  May return one or more empty values while skipping "
               << " this long line.";
    reading_a_long_line_ = true;
    return true;  // Skip the current part of a long line.
  } else {
    ++line_num_;
    if (reading_a_long_line_) {
      reading_a_long_line_ = false;
      return true;  // Skip the last part of a long line.
    }
  }

  if (line_[read_size - 1] == '\n') {
    line_[read_size - 1] = '\0';
    if (read_size > 1 && line_[read_size - 2] == '\r') {  // Handle DOS
                                                          // text format.
      line_[read_size - 2] = '\0';
    }
  }
  value->assign(line_.get());
  return true;
}

//-----------------------------------------------------------------------------
// Implementation of ProtoRecordReader
//-----------------------------------------------------------------------------
bool ProtoRecordReader::Read(std::string* key, std::string* value) {
  if (input_stream_ == NULL) {
    return false;
  }
  return protofile::ReadRecord(input_stream_, key, value);
}

}  // namespace mapreduce_lite
