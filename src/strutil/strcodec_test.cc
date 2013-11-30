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
// Author: yiwang@tencent.com (Yi Wang)
//
#include "gtest/gtest.h"

#include "src/strutil/strcodec.h"

TEST(StrCodecTest, testFastCodecInt32) {
  std::string str;
  EncodeInt32(0, &str);
  EXPECT_EQ(0, DecodeInt32(str));

  EncodeInt32(1, &str);
  EXPECT_EQ(1, DecodeInt32(str));

  EncodeInt32(-1, &str);
  EXPECT_EQ(-1, DecodeInt32(str));

  EncodeInt32(0xffffffff, &str);
  EXPECT_EQ(0xffffffff, DecodeInt32(str));
}

TEST(StrCodecTest, testFastCodecUint64) {
  std::string str;
  EncodeUint64(0, &str);
  EXPECT_EQ(0, DecodeUint64(str));

  EncodeUint64(1, &str);
  EXPECT_EQ(1, DecodeUint64(str));

  EncodeUint64(0xffffffffffffffffLLU, &str);
  EXPECT_EQ(0xffffffffffffffffLLU, DecodeUint64(str));
}

TEST(StrCodecTest, testInt32ToKey) {
  std::string key;
  Int32ToKey(0, &key);
  EXPECT_EQ(key, "0000000000");

  Int32ToKey(1, &key);
  EXPECT_EQ(key, "0000000001");
}

TEST(StrCodecTest, testKeyToInt32) {
  std::string key;
  Int32ToKey(0, &key);
  EXPECT_EQ(KeyToInt32(key), 0);

  Int32ToKey(1, &key);
  EXPECT_EQ(KeyToInt32(key), 1);
}
