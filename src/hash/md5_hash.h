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
// Author: Zihan Liu (updogliu@tencent.com)
//
// This file exports the MD5 hashing algorithm.
//
#ifndef HASH_MD5_HASH_H_
#define HASH_MD5_HASH_H_

#include <string>

#include "src/base/common.h"

uint64 MD5Hash(const unsigned char *s, const unsigned int len);
uint64 MD5Hash(const std::string& s);

#endif  // HASH_MD5_HASH_H_
