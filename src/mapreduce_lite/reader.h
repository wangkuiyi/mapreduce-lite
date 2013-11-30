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
// Define the interface of Reader and two standard readers: TextReader
// and ProtoRecordReader.
//
#ifndef MAPREDUCE_LITE_READERS_H_
#define MAPREDUCE_LITE_READERS_H_

#include <stdio.h>
#include <string>

#include "src/base/class_register.h"
#include "src/base/scoped_ptr.h"

namespace mapreduce_lite {

//-----------------------------------------------------------------------------
// The interface implemented by ``real'' readers.
//-----------------------------------------------------------------------------
class Reader {
 public:
  Reader() { input_stream_ = NULL; }
  virtual ~Reader() { Close(); }

  virtual void Open(const std::string& source_name);
  virtual void Close();

  // Returns false to indicate that the current read failed and no
  // further reading operations should be performed.
  virtual bool Read(std::string* key, std::string* value) = 0;

 protected:
  std::string input_filename_;
  FILE* input_stream_;
};


//-----------------------------------------------------------------------------
// Read each record as a line in a text file.
// - The key returned by Read() is "filename-offset", the value
//   returned by Read is the content of a line.
// - The value might be empty if it is reading a too long line.
// - The '\r' (if there is any) and '\n' at the end of a line are
//   removed.
//-----------------------------------------------------------------------------
class TextReader : public Reader {
 public:
  TextReader();
  virtual bool Read(std::string* key, std::string* value);
 private:
  scoped_array<char> line_;      // input line buffer
  int line_num_;                 // count line number
  bool reading_a_long_line_;     // is reading a lone line
};


//-----------------------------------------------------------------------------
// Read each record as a KeyValuePair proto message in a protofile.
//-----------------------------------------------------------------------------
class ProtoRecordReader : public Reader {
 public:
  virtual bool Read(std::string* key, std::string* value);
};


CLASS_REGISTER_DEFINE_REGISTRY(mapreduce_lite_reader_registry, Reader);

#define REGISTER_READER(format_name, reader_name)       \
  CLASS_REGISTER_OBJECT_CREATOR(                        \
      mapreduce_lite_reader_registry,                   \
      Reader,                                           \
      format_name,                                      \
      reader_name)

#define CREATE_READER(format_name)              \
  CLASS_REGISTER_CREATE_OBJECT(                 \
      mapreduce_lite_reader_registry,           \
      format_name)

}  // namespace mapreduce_lite

#endif  // MAPREDUCE_LITE_READERS_H_
