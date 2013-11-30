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
#include "src/base/varint32.h"

#include <stdio.h>

#include "src/base/common.h"
#include "google/protobuf/io/coded_stream.h"

namespace {
inline uint32 GetByte(FILE* input) {
  return static_cast<uint32>(getc(input));
}
}  // namespace

// The following varint32 decoding code snippet is copied from an
// internal function of Google protobuf.  It is compliant with the
// varint32 codec specification at
// http://code.google.com/apis/protocolbuffers/docs/encoding.html
bool ReadVarint32(FILE* input, uint32* value) {
  static const int kMaxVarintBytes = 10;
  static const int kMaxVarint32Bytes = 5;

  if (ferror(input) || feof(input))
    return false;

  uint32 b;
  uint32 result;

  b = GetByte(input);
  result  = (b & 0x7F);
  if (!(b & 0x80)) goto done;

  b = GetByte(input);
  result |= (b & 0x7F) <<  7;
  if (!(b & 0x80)) goto done;

  b = GetByte(input);
  result |= (b & 0x7F) << 14;
  if (!(b & 0x80)) goto done;

  b = GetByte(input);
  result |= (b & 0x7F) << 21;
  if (!(b & 0x80)) goto done;

  b = GetByte(input);
  result |=  b         << 28;
  if (!(b & 0x80)) goto done;

  // If the input is larger than 32 bits, we still need to read it all
  // and discard the high-order bits.
  for (int i = 0; i < kMaxVarintBytes - kMaxVarint32Bytes; i++) {
    b = GetByte(input);
    if (!(b & 0x80)) goto done;
  }

  // We have overrun the maximum size of a varint (10 bytes).  Assume
  // the data is corrupt.
  return false;

 done:
  *value = result;
  return true;
}


bool WriteVarint32(FILE* output, uint32 value) {
  using google::protobuf::io::CodedOutputStream;
  uint8 buffer[4];
  uint8* end = CodedOutputStream::WriteVarint32ToArray(value, buffer);
  return fwrite(buffer, 1, end - buffer, output) == (end - buffer);
}


