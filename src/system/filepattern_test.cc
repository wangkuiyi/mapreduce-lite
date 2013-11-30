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
// Copyright 2011 Tencent Inc.
// Author: Yi Wang (yiwang@tencent.com)

#include <stdio.h>                      // For fopen() and remove()

#include <string>

#include "gtest/gtest.h"
#include "src/strutil/stringprintf.h"
#include "src/system/filepattern.h"

static const int kNumTestFiles = 5;
static const char* kTestFilebase = "/tmp/filepattern-test";

class FilepatternMatcherTest : public ::testing::Test {
 protected:
  virtual void SetUp() {
    FILE* file = NULL;
    for (int i = 0; i < kNumTestFiles; ++i) {
      std::string filename;
      SStringPrintf(&filename, "%s-%05d-of-%05d", kTestFilebase,
                    i, kNumTestFiles);
      ASSERT_TRUE((file = fopen(filename.c_str(), "w+")) != NULL);
    }
  }

  virtual void TearDown() {
    for (int i = 0; i < kNumTestFiles; ++i) {
      std::string filename;
      SStringPrintf(&filename, "%s-%05d-of-%05d", kTestFilebase,
                    i, kNumTestFiles);
      ASSERT_EQ(remove(filename.c_str()), 0);
    }
  }
};

TEST_F(FilepatternMatcherTest, MatchUsingAsterisk) {
  FilepatternMatcher m(
      StringPrintf("%s-*-of-%05d", kTestFilebase, kNumTestFiles));
  EXPECT_EQ(m.NumMatched(), 5);
  EXPECT_TRUE(m.NoError());
}

TEST_F(FilepatternMatcherTest, MatchUsingQuestionMark) {
  FilepatternMatcher m(
      StringPrintf("%s-0000?-of-%05d", kTestFilebase, kNumTestFiles));
  EXPECT_EQ(m.NumMatched(), 5);
  EXPECT_TRUE(m.NoError());
}

TEST_F(FilepatternMatcherTest, MatchUsingSpecifiedRange) {
  FilepatternMatcher m(
      StringPrintf("%s-0000[0-2]-of-%05d", kTestFilebase, kNumTestFiles));
  EXPECT_EQ(m.NumMatched(), 3);
  EXPECT_TRUE(m.NoError());
}

TEST_F(FilepatternMatcherTest, MatchUsingBrace) {
  FilepatternMatcher m(
      StringPrintf("%s-000{00,01,02}-of-%05d", kTestFilebase, kNumTestFiles));
  EXPECT_EQ(m.NumMatched(), 3);
  EXPECT_TRUE(m.NoError());
}

TEST_F(FilepatternMatcherTest, MatchASpecificFile) {
  FilepatternMatcher m(
      StringPrintf("%s-00000-of-%05d", kTestFilebase, kNumTestFiles));
  EXPECT_EQ(m.NumMatched(), 1);
  EXPECT_TRUE(m.NoError());
}

TEST_F(FilepatternMatcherTest, MatchNotExisting) {
  FilepatternMatcher m("/tmp/somthing-that-does-not-seem-possible-to-exist");
  EXPECT_EQ(m.NumMatched(), 0);
  EXPECT_FALSE(m.NoError());
}

