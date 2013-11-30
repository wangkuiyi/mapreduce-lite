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
#include "src/sorted_buffer/sorted_buffer_iterator.h"

#include "gtest/gtest.h"

#include "src/base/common.h"
#include "src/base/varint32.h"
#include "src/sorted_buffer/sorted_buffer.h"

namespace sorted_buffer {

TEST(SortedBufferIteratorTest, SortedBufferIterator) {
  // The following code snippet that generates a series of two disk
  // block files are copied from sorted_buffer_test.cc
  //
  static const std::string kTmpFilebase("/tmp/testSortedBufferIterator");
  static const int kInMemBufferSize = 40;  // Can hold two key-value pairs
  static const std::string kSomeStrings[] = {
    "applee", "applee", "applee", "papaya" };
  static const std::string kValue("123456");
  {
    SortedBuffer buffer(kTmpFilebase, kInMemBufferSize);
    for (int k = 0; k < sizeof(kSomeStrings)/sizeof(kSomeStrings[0]); ++k) {
      buffer.Insert(kSomeStrings[k], kValue);
    }
    buffer.Flush();
  }

  int i = 0;
  for (SortedBufferIteratorImpl iter(kTmpFilebase, 2); !iter.FinishedAll();
       iter.NextKey()) {
    for (; !iter.Done(); iter.Next()) {
      EXPECT_EQ(iter.key(), kSomeStrings[i]);
      EXPECT_EQ(iter.value(), kValue);
      ++i;
    }
  }
}

}  // namespace sorted_buffer
