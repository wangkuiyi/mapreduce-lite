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

#include "src/base/varint32.h"

#include <fstream>

#include "gtest/gtest.h"

#include "src/base/common.h"

TEST(Varint32Test, WriteAndReadVarint32) {
  static const char* kTmpFile = "/tmp/varint32_test.tmp";

  uint32 kTestValues[] = { 0, 1, 0xff, 0xffff, 0xffffffff };

  FILE* output = fopen(kTmpFile, "w+");
  for (int i = 0; i < sizeof(kTestValues)/sizeof(kTestValues[0]); ++i) {
    if (!WriteVarint32(output, kTestValues[i])) {
      LOG(FATAL) << "Error on WriteVarint32 with value= " << kTestValues[i];
    }
  }
  fclose(output);

  FILE* input = fopen(kTmpFile, "r");
  for (int i = 0; i < sizeof(kTestValues)/sizeof(kTestValues[0]); ++i) {
    uint32 value;
    if (!ReadVarint32(input, &value)) {
      LOG(FATAL) << "Error on ReadVarint32 with value = " << kTestValues[i];
    }
    EXPECT_EQ(kTestValues[i], value);
  }
  fclose(input);
}
