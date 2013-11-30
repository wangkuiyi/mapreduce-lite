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
#include "src/strutil/join_strings.h"

#include <list>
#include <set>
#include <string>
#include <vector>

#include "gtest/gtest.h"

TEST(JoinStringsTest, JoinStringsInVector) {
  std::vector<std::string> vector;
  vector.push_back("apple");
  vector.push_back("banana");
  vector.push_back("orange");

  std::string output;
  JoinStrings(vector.begin(), vector.end(), ",", &output);
  EXPECT_EQ("apple,banana,orange", output);
  EXPECT_EQ("apple,banana,orange",
            JoinStrings(vector.begin(), vector.end(), ","));
}

TEST(JoinStringsTest, JoinStringsInList) {
  std::list<std::string> list;
  list.push_back("apple");
  list.push_back("banana");
  list.push_back("orange");

  std::string output;
  JoinStrings(list.begin(), list.end(), ",", &output);
  EXPECT_EQ("apple,banana,orange", output);
  EXPECT_EQ("apple,banana,orange",
            JoinStrings(list.begin(), list.end(), ","));
}

TEST(JoinStringsTest, JoinStringsInSet) {
  std::set<std::string> set;
  set.insert("apple");
  set.insert("banana");
  set.insert("orange");

  std::string output;
  JoinStrings(set.begin(), set.end(), ",", &output);
  EXPECT_EQ("apple,banana,orange", output);
  EXPECT_EQ("apple,banana,orange",
            JoinStrings(set.begin(), set.end(), ","));
}
