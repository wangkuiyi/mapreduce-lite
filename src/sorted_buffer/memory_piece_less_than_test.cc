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

#include <map>

#include "gtest/gtest.h"

#include "src/sorted_buffer/sorted_buffer.h"

namespace sorted_buffer {

TEST(MemoryPieceLessThanTest, LessThan) {
  MemoryPieceLessThan lt;

  char buffer1[] = "1234apple";
  MemoryPiece p1(buffer1, strlen(buffer1) - sizeof(PieceSize));

  char buffer2[] = "1234applee";
  MemoryPiece p2(buffer2, strlen(buffer2) - sizeof(PieceSize));

  EXPECT_TRUE(lt(p1, p2));
  EXPECT_TRUE(!lt(p2, p1));
}

TEST(MemoryPieceLessThanTest, LessThanNULLPiece) {
  MemoryPieceLessThan lt;

  char buffer1[sizeof(PieceSize)];
  MemoryPiece p1(buffer1, 0);

  char buffer2[sizeof(PieceSize)];
  MemoryPiece p2(buffer2, 0);

  char buffer3[] = "1234applee";
  MemoryPiece p3(buffer3, strlen(buffer3) - sizeof(PieceSize));

  EXPECT_TRUE(!lt(p1, p2));
  EXPECT_TRUE(!lt(p2, p1));
  EXPECT_TRUE(lt(p1, p3));
  EXPECT_TRUE(lt(p2, p3));
  EXPECT_TRUE(!lt(p3, p3));
}

TEST(MemoryPieceLessThanTest, LessThanInSTLContainer) {
  typedef std::map<MemoryPiece, /*key*/
                   int,         /*value*/
                   MemoryPieceLessThan> MemoryPieceMap;
  MemoryPieceMap m;

  char buffer1[] = "1234apple";
  MemoryPiece p(buffer1, strlen(buffer1) - sizeof(PieceSize));

  m[p] = 2;
  CHECK(m.find(p) != m.end());
  CHECK_EQ(m.find(p)->second, 2);

  char buffer2[] = "1234apple";
  p.Set(buffer2, strlen(buffer2) - sizeof(PieceSize));
  CHECK(m.find(p) != m.end());
  CHECK_EQ(m.find(p)->second, 2);

  std::string s("apple");
  p.Set(&s);
  CHECK(m.find(p) != m.end());
  CHECK_EQ(m.find(p)->second, 2);

  MemoryPiece p1(&s);
  CHECK(m.find(p1) != m.end());
  CHECK_EQ(m.find(p1)->second, 2);
}

}  // namespace sorted_buffer
