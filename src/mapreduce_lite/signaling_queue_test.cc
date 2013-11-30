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

#include "src/mapreduce_lite/signaling_queue.h"

#include <string>
#include "gtest/gtest.h"

using std::string;
using mapreduce_lite::SignalingQueue;

TEST(SignalingQueueTest, AddRemove) {
  SignalingQueue queue(5, 1);  // size:5, num_of_producer:1
  char buff[10];
  queue.Add("111", 3);
  queue.Add("22", 2);
  EXPECT_EQ(0, queue.Add("xxxx", 4, false));  // non-blocking add
  queue.Remove(buff, 3);
  EXPECT_EQ(string(buff, 3), string("111"));
  queue.Remove(buff, 2);
  EXPECT_EQ(string(buff, 2), string("22"));
  queue.Add("33333", 5);
  queue.Remove(buff, 5);
  EXPECT_EQ(string(buff, 5), string("33333"));
  EXPECT_EQ(0, queue.Remove(buff, 10, false));  // non-blocking remove
  EXPECT_EQ(queue.Add("666666", 6), -1);  // exceed buffer size
  queue.Add("55555", 5);
  EXPECT_EQ(queue.Remove(buff, 3), -1);  // message too long
}

TEST(SignalingQueueTest, EmptyAndNoMoreAdd) {
  SignalingQueue queue(5, 2);  // size:5, num_of_producer:2
  char buff[10];
  EXPECT_EQ(queue.EmptyAndNoMoreAdd(), false);
  queue.Signal(1);
  queue.Signal(1);
  EXPECT_EQ(queue.EmptyAndNoMoreAdd(), false);
  queue.Signal(2);
  EXPECT_EQ(queue.EmptyAndNoMoreAdd(), true);
  EXPECT_EQ(queue.Remove(buff, 5), 0);
}

