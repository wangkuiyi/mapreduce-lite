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
#include "src/hash/simple_hash.h"

#include <string>

unsigned int RSHash(const std::string& str) {
  unsigned int b    = 378551;
  unsigned int a    = 63689;
  unsigned int hash = 0;

  for (std::size_t i = 0; i < str.length(); i++) {
    hash = hash * a + str[i];
    a = a * b;
  }
  return hash;
}
/* End Of RS Hash Function */


unsigned int JSHash(const std::string& str) {
  unsigned int hash = 1315423911;

  for (std::size_t i = 0; i < str.length(); i++) {
    hash ^= ((hash << 5) + str[i] + (hash >> 2));
  }
  return hash;
}
/* End Of JS Hash Function */


unsigned int PJWHash(const std::string& str) {
  unsigned int BitsInUnsignedInt = (unsigned int)(sizeof(unsigned int) * 8);
  unsigned int ThreeQuarters = (unsigned int)((BitsInUnsignedInt  * 3) / 4);
  unsigned int OneEighth     = (unsigned int)(BitsInUnsignedInt / 8);
  unsigned int HighBits =
      (unsigned int)(0xFFFFFFFF) << (BitsInUnsignedInt - OneEighth);
  unsigned int hash              = 0;
  unsigned int test              = 0;

  for (std::size_t i = 0; i < str.length(); i++) {
    hash = (hash << OneEighth) + str[i];

    if ((test = hash & HighBits) != 0) {
      hash = ((hash ^ (test >> ThreeQuarters)) & (~HighBits));
    }
  }

  return hash;
}
/* End Of  P. J. Weinberger Hash Function */


unsigned int ELFHash(const std::string& str) {
  unsigned int hash = 0;
  unsigned int x    = 0;

  for (std::size_t i = 0; i < str.length(); i++) {
    hash = (hash << 4) + str[i];
    if ((x = hash & 0xF0000000L) != 0) {
      hash ^= (x >> 24);
    }
    hash &= ~x;
  }

  return hash;
}
/* End Of ELF Hash Function */


unsigned int BKDRHash(const std::string& str) {
  unsigned int seed = 131;  // 31 131 1313 13131 131313 etc..
  unsigned int hash = 0;

  for (std::size_t i = 0; i < str.length(); i++) {
    hash = (hash * seed) + str[i];
  }

  return hash;
}
/* End Of BKDR Hash Function */


unsigned int SDBMHash(const std::string& str) {
  unsigned int hash = 0;

  for (std::size_t i = 0; i < str.length(); i++) {
    hash = str[i] + (hash << 6) + (hash << 16) - hash;
  }

  return hash;
}
/* End Of SDBM Hash Function */


unsigned int DJBHash(const std::string& str) {
  unsigned int hash = 5381;

  for (std::size_t i = 0; i < str.length(); i++) {
    hash = ((hash << 5) + hash) + str[i];
  }

  return hash;
}
/* End Of DJB Hash Function */


unsigned int DEKHash(const std::string& str) {
  unsigned int hash = static_cast<unsigned int>(str.length());

  for (std::size_t i = 0; i < str.length(); i++) {
    hash = ((hash << 5) ^ (hash >> 27)) ^ str[i];
  }

  return hash;
}
/* End Of DEK Hash Function */


unsigned int BPHash(const std::string& str) {
  unsigned int hash = 0;
  for (std::size_t i = 0; i < str.length(); i++) {
    hash = hash << 7 ^ str[i];
  }

  return hash;
}
/* End Of BP Hash Function */


unsigned int FNVHash(const std::string& str) {
  const unsigned int fnv_prime = 0x811C9DC5;
  unsigned int hash = 0;
  for (std::size_t i = 0; i < str.length(); i++) {
    hash *= fnv_prime;
    hash ^= str[i];
  }

  return hash;
}
/* End Of FNV Hash Function */


unsigned int APHash(const std::string& str) {
  unsigned int hash = 0xAAAAAAAA;

  for (std::size_t i = 0; i < str.length(); i++) {
    hash ^= ((i & 1) == 0) ? ((hash <<  7) ^ str[i] * (hash >> 3)) :
            (~((hash << 11) + (str[i] ^ (hash >> 5))));
  }

  return hash;
}
/* End Of AP Hash Function */
