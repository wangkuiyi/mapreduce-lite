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
// Author: Yi Wang (yi.wang.2005@gmail.com)
//
// Protofile is a legacy file format from MRML. In MRML, it is called
// ``RecordIO'', however, as it differs completely with the well-known
// ``RecordIO'' developed in Google, we rename it ``protofile'' in
// MapReduce Lite.
//
// In general, a protofile is a binary file of records, where each record
// consists of a 4-byte (uint32) record size and a serialized KeyValuePair
// proto message.
//
// Since MapReduce Lite does not inherit MRMLFS, in this version, we
// removed protofile record IO functions working with MRMLFS_File.
//
// We also added an enhancement: to allow both key and value serializations
// of proto messages.
//
#ifndef MAPREDUCE_LITE_PROTOFILE_H_
#define MAPREDUCE_LITE_PROTOFILE_H_

#include <stdio.h>

#include <string>

namespace google {
namespace protobuf {
class Message;
}
}

namespace mapreduce_lite {
namespace protofile {

bool ReadRecord(FILE* input,
                std::string* key,
                std::string* value);
bool ReadRecord(FILE* input,
                std::string* key,
                ::google::protobuf::Message* value);
bool ReadRecord(FILE* input,
                ::google::protobuf::Message* key,
                std::string* value);
bool ReadRecord(FILE* input,
                ::google::protobuf::Message* key,
                ::google::protobuf::Message* value);

bool WriteRecord(FILE* output,
                 const std::string& key,
                 const std::string& value);
bool WriteRecord(FILE* output,
                 const std::string& key,
                 const ::google::protobuf::Message& value);
bool WriteRecord(FILE* output,
                 const ::google::protobuf::Message& key,
                 const std::string& value);
bool WriteRecord(FILE* output,
                 const ::google::protobuf::Message& key,
                 const ::google::protobuf::Message& value);

}  // namespace protofile
}  // namespace mapreduce_lite

#endif  // MAPREDUCE_LITE_PROTOFILE_H_
