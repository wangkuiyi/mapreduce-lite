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
#include <gtest/gtest.h>

#include "src/sorted_buffer/memory_piece.h"

using sorted_buffer::MemoryPiece;
using sorted_buffer::PieceSize;

TEST(MemoryPieceTest, SetMemoryPiece) {
  MemoryPiece p;
  CHECK(!p.IsSet());

  char buffer[1024];
  p.Set(buffer, 100);
  CHECK(p.IsSet());
  CHECK_EQ(p.Piece(), buffer);
  CHECK_EQ(p.Size(), 100);
  CHECK_EQ(p.Data(), buffer + sizeof(PieceSize));
}

TEST(MemoryPieceTest, ConstructNULLPiece) {
  char buffer[sizeof(PieceSize)];
  MemoryPiece p(buffer, 0);
  CHECK(p.IsSet());
  CHECK_EQ(p.Piece(), buffer);
  CHECK_EQ(p.Size(), 0);
  CHECK_EQ(p.Data(), buffer + sizeof(PieceSize));
}

TEST(MemoryPieceTest, ConstructMemoryPiece) {
  char buffer[1024];
  MemoryPiece p(buffer, 100);
  CHECK(p.IsSet());
  CHECK_EQ(p.Piece(), buffer);
  CHECK_EQ(p.Size(), 100);
  CHECK_EQ(p.Data(), buffer + sizeof(PieceSize));
}
