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
// Copyright 2010
// Author: yiwang@tencent.com (Yi Wang)
//
// This file declares functions that converts numerical types
// from/into binary forms.  An example usage of these functions is
// generating/parsing MapReduce keys from/as numerical types.
//
#ifndef STRUTIL_STRCODEC_H_
#define STRUTIL_STRCODEC_H_

#include <string>
#include "src/base/common.h"

// Following functions encode numerical values in human-readable format.

void Int32ToKey(int32 value, std::string* str);
std::string Int32ToKey(int32 value);

void Uint32ToKey(uint32 value, std::string* str);
std::string Uint32ToKey(uint32 value);

void Int64ToKey(int64 value, std::string* str);
std::string Int64ToKey(int64 value);

void Uint64ToKey(uint64 value, std::string* str);
std::string Uint64ToKey(uint64 value);

int32  KeyToInt32(const std::string& key);
uint32 KeyToUint32(const std::string& key);
int64  KeyToInt64(const std::string& key);
uint64 KeyToUint64(const std::string& key);

// Following functions does fast encoding/decoding of numerical types.
// NOTE: The fast encoding of a numerical value is just its memory
// mirro, and MUST NOT be passed cross machines using different endian
// styles.

void EncodeInt32(int32 value, std::string* str);
std::string EncodeInt32(int32 value);

int32 DecodeInt32(const std::string& str);

void EncodeUint32(uint32 value, std::string* str);
std::string EncodeUint32(uint32 value);

uint32 DecodeUint32(const std::string& str);

void EncodeInt64(int64 value, std::string* str);
std::string EncodeInt64(int64 value);

int64 DecodeInt64(const std::string& str);

void EncodeUint64(uint64 value, std::string* str);
std::string EncodeUint64(uint64 value);

uint64 DecodeUint64(const std::string& str);

#endif  // STRUTIL_STRCODEC_H_
