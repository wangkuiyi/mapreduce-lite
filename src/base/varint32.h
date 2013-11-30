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
// Implementation the codec of varint32 using code from Google Protobuf.
//
#ifndef BASE_VARINT32_H_
#define BASE_VARINT32_H_

#include "src/base/common.h"

bool ReadVarint32(FILE* input, uint32* value);
bool WriteVarint32(FILE* output, uint32 value);

#endif  // BASE_VARINT32_H_
