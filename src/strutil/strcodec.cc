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
#include <string.h>             // for memcpy

#include <iomanip>
#include <sstream>

#include "src/base/common.h"
#include "src/strutil/strcodec.h"
#include "src/strutil/stringprintf.h"

// |XXXToKey| functions will generate a string with length
// kNumericValueFillSize containing human readable format of a
// numerical value, prefixed by '0's.
const int kNumericValueFillSize = 10;

//-----------------------------------------------------------------------------
// Template implementation of human-readable encoding.
//-----------------------------------------------------------------------------
template <typename ValueType>
void NumericValueToKey(const ValueType& value, std::string* key) {
  using std::ostringstream;
  CHECK_GE(value, 0);
  ostringstream o;
  o << std::setfill('0') << std::setw(kNumericValueFillSize) << value;
  *key = o.str();
}

template <typename ValueType>
std::string NumericValueToKey(const ValueType& value) {
  using std::ostringstream;
  CHECK_GE(value, 0);
  ostringstream o;
  o << std::setfill('0') << std::setw(kNumericValueFillSize) << value;
  return o.str();
}

template <typename ValueType>
ValueType KeyToNumericValue(const std::string& key) {
  using std::istringstream;
  istringstream i(key);
  ValueType v;
  i >> v;
  return v;
}

//-----------------------------------------------------------------------------
// Realizations of fast human-readable encoding.
//-----------------------------------------------------------------------------

void Int32ToKey(int32 value, std::string* str) {
  NumericValueToKey<int32>(value, str);
}

std::string Int32ToKey(int32 value) {
  return NumericValueToKey<int32>(value);
}

void Uint32ToKey(uint32 value, std::string* str) {
  NumericValueToKey<int32>(value, str);
}

std::string Uint32ToKey(uint32 value) {
  return NumericValueToKey<int32>(value);
}

void Int64ToKey(int64 value, std::string* str) {
  NumericValueToKey<int32>(value, str);
}

std::string Int64ToKey(int64 value) {
  return NumericValueToKey<int32>(value);
}

void Uint64ToKey(uint64 value, std::string* str) {
  NumericValueToKey<int32>(value, str);
}

std::string Uint64ToKey(uint64 value) {
  return NumericValueToKey<int32>(value);
}

int32  KeyToInt32(const std::string& key) {
  return KeyToNumericValue<int32>(key);
}

uint32 KeyToUint32(const std::string& key) {
  return KeyToNumericValue<uint32>(key);
}

int64  KeyToInt64(const std::string& key) {
  return KeyToNumericValue<int64>(key);
}

uint64 KeyToUint64(const std::string& key) {
  return KeyToNumericValue<uint64>(key);
}

//-----------------------------------------------------------------------------
// Template implementation of fast encoding/decoding.
//-----------------------------------------------------------------------------
template <typename ValueType>
void EncodeNumericValue(const ValueType& value, std::string* str) {
  str->resize(sizeof(ValueType));
  memcpy(const_cast<char*>(str->data()), &value, sizeof(ValueType));
}

template <typename ValueType>
std::string EncodeNumericValue(const ValueType& value) {
  std::string str;
  str.resize(sizeof(ValueType));
  memcpy(const_cast<char*>(str.data()), &value, sizeof(ValueType));
  return str;
}

template <typename ValueType>
ValueType DecodeNumericValue(const std::string& str) {
  ValueType ret;
  CHECK_EQ(str.size(), sizeof(ValueType));
  memcpy(&ret, str.data(), sizeof(ValueType));
  return ret;
}

//-----------------------------------------------------------------------------
// Realizations of fast encoding/decoding.
//-----------------------------------------------------------------------------
void EncodeInt32(int32 value, std::string* str) {
  EncodeNumericValue<int32>(value, str);
}

std::string EncodeInt32(int32 value) {
  return EncodeNumericValue<int32>(value);
}

int32 DecodeInt32(const std::string& str) {
  return DecodeNumericValue<int32>(str);
}

void EncodeUint32(uint32 value, std::string* str) {
  EncodeNumericValue<uint32>(value, str);
}

std::string EncodeUint32(uint32 value) {
  return EncodeNumericValue<uint32>(value);
}

uint32 DecodeUint32(const std::string& str) {
  return DecodeNumericValue<uint32>(str);
}

void EncodeInt64(int64 value, std::string* str) {
  EncodeNumericValue<int64>(value, str);
}

std::string EncodeInt64(int64 value) {
  return EncodeNumericValue<int64>(value);
}

int64 DecodeInt64(const std::string& str) {
  return DecodeNumericValue<int64>(str);
}

void EncodeUint64(uint64 value, std::string* str) {
  EncodeNumericValue<uint64>(value, str);
}

std::string EncodeUint64(uint64 value) {
  return EncodeNumericValue<uint64>(value);
}

uint64 DecodeUint64(const std::string& str) {
  return DecodeNumericValue<uint64>(str);
}
