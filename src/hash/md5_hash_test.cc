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
// Author: Yi Wang (yiwang@tencet.com)
//
// This unittest uses grouth-truth MD5 results from Wikipedia:
// http://en.wikipedia.org/wiki/MD5#MD5_hashes.

#include <stdio.h>

#include "gtest/gtest.h"

#include "src/base/common.h"
#include "src/hash/md5_hash.h"

TEST(MD5Test, AsGroundTruthOnWikipedia) {
  EXPECT_EQ(MD5Hash("The quick brown fox jumps over the lazy dog"),
            0x82b62b379d7d109eLLU);
  EXPECT_EQ(MD5Hash("The quick brown fox jumps over the lazy dog."),
            0x1cfbd090c209d9e4LLU);
  EXPECT_EQ(MD5Hash(""), 0x04b2008fd98c1dd4LLU);
}
