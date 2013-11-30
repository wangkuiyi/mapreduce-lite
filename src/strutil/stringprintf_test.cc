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
// Author: Lei Shen (leoshen@tencent.com)
//
#include "src/strutil/stringprintf.h"

#include "gtest/gtest.h"

TEST(StringPrintf, normal) {
  using std::string;
  EXPECT_EQ(StringPrintf("%d", 1), string("1"));
  string target;
  SStringPrintf(&target, "%d", 1);
  EXPECT_EQ(target, string("1"));
  StringAppendF(&target, "%d", 2);
  EXPECT_EQ(target, string("12"));
}
