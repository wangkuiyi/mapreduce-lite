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
// Author: YAN Hao (charlieyan@tencent.com)

#include "src/mapreduce_lite/socket_communicator.h"

#include <unistd.h>
#include <iostream>
#include <string>
#include <vector>

#include "gtest/gtest.h"
#include "src/strutil/split_string.h"

using std::cout;
using std::endl;
using std::string;
using std::vector;
using mapreduce_lite::SocketCommunicator;

const int kNumMessage = 100;

// this test forks 4 processes:
// 2 map workers and 2 reduce workers
TEST(CommunicatorTest, SendRecv) {
  int pid;
  pid = fork();
  ASSERT_LE(0, pid);
  vector<string> reducers;
  SplitStringUsing(
      string("127.0.0.1:10103,127.0.0.1:10104"),
      ",",
      &reducers);

  if (pid > 0) {  // parent: 2 reducers
    pid = fork();
    ASSERT_LE(0, pid);
    if (pid > 0) {  // reducer 0
      InitializeLogger("/tmp/r0-info", "/tmp/r0-warn", "/tmp/r0-erro");
      SocketCommunicator r0;
      ASSERT_TRUE(r0.Initialize(false,
                            2,
                            reducers,
                            1024, 1024,
                            0));
      string result;
      int message_count_0 = 0;
      int message_count_1 = 0;
      while (true) {
        int retval = r0.Receive(&result);
        ASSERT_LE(0, retval);
        if (0 == retval) break;
        if (result[1] == '0') {
          ASSERT_EQ(string("m0r0"), result);
          ++message_count_0;
        } else {
          ASSERT_EQ(string("m11r0"), result);
          ++message_count_1;
        }
      }
      EXPECT_EQ(kNumMessage, message_count_0);
      EXPECT_EQ(kNumMessage, message_count_1);
      ASSERT_TRUE(r0.Finalize());
    } else {  // reducer 1
      InitializeLogger("/tmp/r1-info", "/tmp/r1-warn", "/tmp/r1-erro");
      sleep(1);
      SocketCommunicator r1;
      ASSERT_TRUE(r1.Initialize(false,
                            2,
                            reducers,
                            1024, 1024,
                            1));
      string result;
      int message_count_0 = 0;
      int message_count_1 = 0;
      while (true) {
        int retval = r1.Receive(&result);
        ASSERT_LE(0, retval);
        if (0 == retval) break;
        if (result[1] == '0') {
          ASSERT_EQ(string("m0r11"), result);
          ++message_count_0;
        } else {
          ASSERT_EQ(string("m11r11"), result);
          ++message_count_1;
        }
      }
      EXPECT_EQ(kNumMessage, message_count_0);
      EXPECT_EQ(kNumMessage, message_count_1);
      ASSERT_TRUE(r1.Finalize());
    }
  } else {  // child: 2 mappers
    sleep(2);
    pid = fork();
    ASSERT_LE(0, pid);
    if (pid > 0) {  // mapper 0
      InitializeLogger("/tmp/m0-info", "/tmp/m0-warn", "/tmp/m0-erro");
      SocketCommunicator m0;
      ASSERT_TRUE(m0.Initialize(true,
                            2,
                            reducers,
                            1024, 1024,
                            0));
      for (int i = 0; i < kNumMessage; ++i) {
        EXPECT_LE(0, m0.Send(string("m0r0"), 0));
        EXPECT_LE(0, m0.Send(string("m0r11"), 1));
      }
      ASSERT_TRUE(m0.Finalize());
    } else {  // mapper 1
      InitializeLogger("/tmp/m1-info", "/tmp/m1-warn", "/tmp/m1-erro");
      SocketCommunicator m1;
      ASSERT_TRUE(m1.Initialize(true,
                            2,
                            reducers,
                            1024, 1024,
                            1));
      for (int i = 0; i < kNumMessage; ++i) {
        EXPECT_LE(0, m1.Send(string("m11r0"), 0));
        EXPECT_LE(0, m1.Send(string("m11r11"), 1));
      }
      ASSERT_TRUE(m1.Finalize());
    }
  }
  wait(0);
}

