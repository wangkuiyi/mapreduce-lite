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
// This source file is mainly copied from http://www.partow.net, with
// the following Copyright information.
/*
 **************************************************************************
 *                                                                        *
 *          General Purpose Hash Function Algorithms Library              *
 *                                                                        *
 * Author: Arash Partow - 2002                                            *
 * URL: http://www.partow.net                                             *
 * URL: http://www.partow.net/programming/hashfunctions/index.html        *
 *                                                                        *
 * Copyright notice:                                                      *
 * Free use of the General Purpose Hash Function Algorithms Library is    *
 * permitted under the guidelines and in accordance with the most current *
 * version of the Common Public License.                                  *
 * http://www.opensource.org/licenses/cpl1.0.php                          *
 *                                                                        *
 **************************************************************************
*/
#ifndef HASH_SIMPLE_HASH_H_
#define HASH_SIMPLE_HASH_H_

#include <string>

#include "src/base/common.h"

typedef unsigned int (*HashFunction)(const std::string&);

unsigned int RSHash(const std::string& str);
unsigned int JSHash(const std::string& str);
unsigned int PJWHash(const std::string& str);
unsigned int ELFHash(const std::string& str);
unsigned int BKDRHash(const std::string& str);
unsigned int SDBMHash(const std::string& str);
unsigned int DJBHash(const std::string& str);
unsigned int DEKHash(const std::string& str);
unsigned int BPHash(const std::string& str);
unsigned int FNVHash(const std::string& str);
unsigned int APHash(const std::string& str);

#endif  // HASH_SIMPLE_HASH_H_
