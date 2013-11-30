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
#include "src/mapreduce_lite/protofile.h"

#include <string>

#include "src/base/common.h"
#include "src/mapreduce_lite/protofile.pb.h"

namespace mapreduce_lite {
namespace protofile {

using std::string;

typedef ::google::protobuf::Message ProtoMessage;

const int kMRMLRecordIOMaxRecordSize = 64*1024*1024;  // 64 MB

//-----------------------------------------------------------------------------
// The following two specializations of function templates parse the read value
// into either (1) an std::string object or (2) a protocol message.
//-----------------------------------------------------------------------------
template <class ValueType>
void ParseValue(KeyValuePair* pair, ValueType* value) {}

template <>
void ParseValue<string>(KeyValuePair* pair, string* value) {
  value->swap(*pair->mutable_value());
}

template <>
void ParseValue<ProtoMessage>(KeyValuePair* pair, ProtoMessage* msg) {
  msg->ParseFromString(pair->value());
}

//-----------------------------------------------------------------------------
// The following two specializations of function templates parse the read key
// into either (1) an std::string object or (2) a protocol message.
//-----------------------------------------------------------------------------
template <class KeyType>
void ParseKey(KeyValuePair* pair, KeyType* key);

template <>
void ParseKey<string>(KeyValuePair* pair, string* key) {
  key->swap(*pair->mutable_key());
}

template <>
void ParseKey<ProtoMessage>(KeyValuePair* pair, ProtoMessage* msg) {
  msg->ParseFromString(pair->key());
}

//-----------------------------------------------------------------------------
// Given ParseKey and ParseValue, the following functiontemplate reads a record
//-----------------------------------------------------------------------------
template <class KeyType, class ValueType>
bool ReadRecord(FILE* input_stream, KeyType* key, ValueType* value) {
  uint32 encoded_msg_size;
  CHECK_EQ(fread(reinterpret_cast<char*>(&encoded_msg_size), 
		 sizeof(encoded_msg_size), 1, input_stream),
	   1);
  if (feof(input_stream) != 0 || ferror(input_stream) != 0) {
    // Do not LOG(ERROR) here, because users do not want to be
    // bothered when they invoke this function in a loop, e.g.,
    // while(MRML_ReadRecord(...)), to probe the end of a record file.
    return false;
  }

  if (encoded_msg_size > kMRMLRecordIOMaxRecordSize) {
    LOG(FATAL) << "Failed to read a proto message with size = "
               << encoded_msg_size
               << ", which is larger than kMRMLRecordIOMaxRecordSize ("
               << kMRMLRecordIOMaxRecordSize << ")."
               << "You can modify kMRMLRecordIOMaxRecordSize defined in "
               << __FILE__;
  }

  static char buffer[kMRMLRecordIOMaxRecordSize];
  CHECK_EQ(fread(buffer, encoded_msg_size, 1, input_stream), 1);
  if (feof(input_stream) != 0 || ferror(input_stream) != 0) {
    LOG(ERROR) << "Error or unexpected EOF in reading protofile.";
    return false;
  }

  KeyValuePair pair;
  CHECK(pair.ParseFromArray(buffer, encoded_msg_size));
  ParseKey<KeyType>(&pair, key);
  ParseValue<ValueType>(&pair, value);
  return true;
}

//-----------------------------------------------------------------------------
// Interfaces defined in mrml_recordio.h are invocations of
// realizations of function template ReadRecord.
//-----------------------------------------------------------------------------

bool ReadRecord(FILE* input, string* key, string* value) {
  return ReadRecord<string, string>(input, key, value);
}

bool ReadRecord(FILE* input, string* key, ProtoMessage* value) {
  return ReadRecord<string, ProtoMessage>(input, key, value);
}

bool ReadRecord(FILE* input, ProtoMessage* key, string* value) {
  return ReadRecord<ProtoMessage, string>(input, key, value);
}

bool ReadRecord(FILE* input, ProtoMessage* key, ProtoMessage* value) {
  return ReadRecord<ProtoMessage, ProtoMessage>(input, key, value);
}

//-----------------------------------------------------------------------------
// The following two specializations of function templates serialize the read
// value into either (1) an std::string object or (2) a protocol message.
//-----------------------------------------------------------------------------
template <class ValueType>
void SerializeValue(KeyValuePair* pair, const ValueType& value);

template <>
void SerializeValue<string>(KeyValuePair* pair, const string& value) {
  pair->set_value(value);
}

template <>
void SerializeValue<ProtoMessage>(KeyValuePair* pair,
                                  const ProtoMessage& msg) {
  msg.SerializeToString(pair->mutable_value());
}

//-----------------------------------------------------------------------------
// The following two specializations of function templates serialize the read
// key into either (1) an std::string object or (2) a protocol message.
//-----------------------------------------------------------------------------
template <class KeyType>
void SerializeKey(KeyValuePair* pair, const KeyType& key);

template <>
void SerializeKey<string>(KeyValuePair* pair, const string& key) {
  pair->set_key(key);
}

template <>
void SerializeKey<ProtoMessage>(KeyValuePair* pair, const ProtoMessage& msg) {
  msg.SerializeToString(pair->mutable_key());
}

//-----------------------------------------------------------------------------
// This function template saves a key-value pair.
//-----------------------------------------------------------------------------
template <class KeyType, class ValueType>
bool WriteRecord(FILE* output_stream,
                 const KeyType& key,
                 const ValueType& value) {
  KeyValuePair pair;
  SerializeKey<KeyType>(&pair, key);
  SerializeValue<ValueType>(&pair, value);
  string encoded_msg;
  CHECK(pair.SerializeToString(&encoded_msg));

  uint32 msg_size = encoded_msg.size();
  size_t ntimes = fwrite(reinterpret_cast<char*>(&msg_size), sizeof(msg_size),
                         1, output_stream);

  if (ntimes != 1 || ferror(output_stream) != 0) {
    LOG(ERROR) << "Failed in writing a record size.";
    return false;
  }

  ntimes = fwrite(const_cast<char*>(encoded_msg.data()), msg_size,
                  1, output_stream);

  if (ntimes != 1 || ferror(output_stream) != 0) {
    LOG(ERROR) << "Failed in writing a record.";
    return false;
  }
  return true;
}

//-----------------------------------------------------------------------------
// Interfaces defined in mrml_recordio.h are invocations of
// realizations of function template ReadRecord.
//-----------------------------------------------------------------------------

bool WriteRecord(FILE* output, const string& key, const string& value) {
  return WriteRecord<string, string>(output, key, value);
}

bool WriteRecord(FILE* output, const string& key, const ProtoMessage& value) {
  return WriteRecord<string, ProtoMessage>(output, key, value);
}

bool WriteRecord(FILE* output, const ProtoMessage& key, const string& value) {
  return WriteRecord<ProtoMessage, string>(output, key, value);
}

bool WriteRecord(FILE* output, const ProtoMessage& key,
                 const ProtoMessage& value) {
  return WriteRecord<ProtoMessage, ProtoMessage>(output, key, value);
}

}  // namespace protofile
}  // namespace mapreduce_lite
