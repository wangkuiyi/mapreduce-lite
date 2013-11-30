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
#include "src/sorted_buffer/memory_allocator.h"

#include "gtest/gtest.h"

namespace sorted_buffer {

class NaiveMemoryAllocatorTest : public ::testing::Test {};

TEST_F(NaiveMemoryAllocatorTest, NaiveMemoryAllocator) {
  NaiveMemoryAllocator a(100);
  CHECK(a.IsInitialized());
  CHECK_EQ(a.PoolSize(), 100);
  CHECK_EQ(a.AllocatedSize(), 0);

  MemoryPiece p;
  CHECK(a.Allocate(50, &p));
  CHECK_EQ(p.Piece(), a.Pool());
  CHECK_EQ(p.Data(), static_cast<const char*>(a.Pool()) + sizeof(PieceSize));
  CHECK_EQ(p.Size(), 50);
  CHECK_EQ(a.AllocatedSize(), 50 + sizeof(PieceSize));
  CHECK_EQ(a.PoolSize(), 100);        // not changed due to allocation

  CHECK(!a.Allocate(50, &p));
  CHECK(p.Piece() == NULL);
  CHECK(p.Data() == NULL);
  CHECK_EQ(p.Size(), 0);
  CHECK_EQ(a.AllocatedSize(), 50 + sizeof(PieceSize));
  CHECK_EQ(a.PoolSize(), 100);        // not changed due to allocation

  CHECK(a.Allocate(50 - 2 * sizeof(PieceSize), &p));
  CHECK_EQ(p.Piece(), a.Pool() + 50 + sizeof(PieceSize));
  CHECK_EQ(p.Data(), a.Pool() + 50 + 2 * sizeof(PieceSize));
  CHECK_EQ(p.Size(), 50 - 2 * sizeof(PieceSize));
  CHECK_EQ(a.AllocatedSize(), a.PoolSize());
  CHECK_EQ(a.PoolSize(), 100);        // not changed due to allocation
}

}  // namespace sorted_buffer
