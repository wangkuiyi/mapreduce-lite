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
#include <vector>
#include <string>

#include "gtest/gtest.h"

#include "src/base/common.h"
#include "src/strutil/split_string.h"

TEST(SplitStringTest, SplitStringUsingCompoundDelim) {
  std::string full(" apple \torange ");
  std::vector<std::string> subs;
  SplitStringUsing(full, " \t", &subs);
  EXPECT_EQ(subs.size(), 2);
  EXPECT_EQ(subs[0], std::string("apple"));
  EXPECT_EQ(subs[1], std::string("orange"));
}

TEST(SplitStringTest, testSplitStringUsingSingleDelim) {
  std::string full(" apple orange ");
  std::vector<std::string> subs;
  SplitStringUsing(full, " ", &subs);
  EXPECT_EQ(subs.size(), 2);
  EXPECT_EQ(subs[0], std::string("apple"));
  EXPECT_EQ(subs[1], std::string("orange"));
}

TEST(SplitStringTest, testSplitingNoDelimString) {
  std::string full("apple");
  std::vector<std::string> subs;
  SplitStringUsing(full, " ", &subs);
  EXPECT_EQ(subs.size(), 1);
  EXPECT_EQ(subs[0], std::string("apple"));
}
