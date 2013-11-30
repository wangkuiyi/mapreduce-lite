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
#include <string.h>

#include <fstream>
#include <string>

#include "gtest/gtest.h"

#include "src/sorted_buffer/memory_piece.h"

namespace sorted_buffer {

TEST(MemoryPieceIOTest, MemoryPieceIO) {
  static const char* kTmpFile = "/tmp/testMemoryPieceIO";

  std::string s("apple");
  std::string empty("");
  char buffer[] = "1234orange";
  char buffer2[sizeof(PieceSize)];

  MemoryPiece p0(&empty);
  MemoryPiece p1(&s);
  MemoryPiece p2(buffer2, 0);
  MemoryPiece p3(buffer, strlen("orange"));

  FILE* output = fopen(kTmpFile, "w+");
  CHECK(output != NULL);
  EXPECT_TRUE(WriteMemoryPiece(output, p0));
  EXPECT_TRUE(WriteMemoryPiece(output, p1));
  EXPECT_TRUE(WriteMemoryPiece(output, p2));
  EXPECT_TRUE(WriteMemoryPiece(output, p3));
  fclose(output);

  FILE* input = fopen(kTmpFile, "r");
  CHECK(input != NULL);
  std::string p;
  EXPECT_TRUE(ReadMemoryPiece(input, &p));
  EXPECT_TRUE(p.empty());
  EXPECT_TRUE(ReadMemoryPiece(input, &p));
  EXPECT_EQ(p, "apple");
  EXPECT_TRUE(ReadMemoryPiece(input, &p));
  EXPECT_TRUE(p.empty());
  EXPECT_TRUE(ReadMemoryPiece(input, &p));
  EXPECT_EQ(p, "orange");
  EXPECT_TRUE(!ReadMemoryPiece(input, &p));
  fclose(input);
}

TEST(MemoryPieceIOTest, ReadFromEmptyFile) {
  static const char* kTmpFile = "/tmp/testReadFromEmptyFile";

  // Create an empty file.
  FILE* output = fopen(kTmpFile, "w+");
  CHECK(output != NULL);
  fclose(output);

  FILE* input = fopen(kTmpFile, "r");
  CHECK(input != NULL);
  std::string p;
  EXPECT_TRUE(!ReadMemoryPiece(input, &p));
  fclose(input);
}

}  // namespace sorted_buffer
