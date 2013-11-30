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
#include <map>

#include "gtest/gtest.h"

#include "src/base/common.h"
#include "src/base/stl-util.h"

TEST(STLUtilTest, DeleteElementsInVector) {
  std::vector<int*> v;
  v.push_back(new int(10));
  v.push_back(new int(20));

  EXPECT_EQ(2, v.size());
  EXPECT_EQ(10, *(v[0]));
  EXPECT_EQ(20, *(v[1]));

  STLDeleteElementsAndClear(&v);

  EXPECT_EQ(0, v.size());
}

TEST(STLUtilTest, DeleteElementsInMap) {
  std::map<int, int*> m;
  m[100] = new int(10);
  m[200] = new int(20);

  EXPECT_EQ(2, m.size());
  EXPECT_EQ(10, *(m[100]));
  EXPECT_EQ(20, *(m[200]));

  STLDeleteValuesAndClear(&m);

  EXPECT_EQ(0, m.size());
}
