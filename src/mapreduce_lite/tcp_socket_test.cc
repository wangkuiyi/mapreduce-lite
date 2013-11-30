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

#include "src/mapreduce_lite/tcp_socket.h"

#include <string.h>
#include <unistd.h>
#include <string>

#include "src/base/common.h"
#include "gtest/gtest.h"

using std::string;
using mapreduce_lite::TCPSocket;

TEST(TCPSocket, SendRecieve) {
  int pid = fork();
  const char * msg = "0123456789";
  ASSERT_GE(pid, 0);
  if (pid > 0) {  // parent: server
    TCPSocket server;
    TCPSocket client;
    string cl_ip;
    uint16 cl_port;
    char serbuff[10];
    memset(serbuff, '\0', 10);

    ASSERT_TRUE(server.Bind("127.0.0.1", 11223));
    ASSERT_TRUE(server.Listen(3));
    ASSERT_TRUE(server.Accept(&client, &cl_ip, &cl_port));

    int tmp;
    int recieved_bytes = 0;
    while (recieved_bytes < 10) {
      tmp = client.Receive(&serbuff[recieved_bytes], 10);
      ASSERT_GE(tmp, 0);
      recieved_bytes += tmp;
    }
    ASSERT_EQ(string("0123456789"), string(serbuff, 10));
    int sent_bytes = 0;
    while (sent_bytes < 10) {
      tmp = client.Send(&msg[sent_bytes], 3);
      ASSERT_GE(tmp, 0);
      sent_bytes += tmp;
    }
  } else {  // child: client
    sleep(3);   // wait for server
    TCPSocket client;
    ASSERT_TRUE(client.Connect("127.0.0.1", 11223));
    char clibuff[10];
    memset(clibuff, '\0', 10);

    int tmp;
    int sent_bytes = 0;
    while (sent_bytes < 10) {
      tmp = client.Send(&msg[sent_bytes], 3);
      ASSERT_GE(tmp, 0);
      sent_bytes += tmp;
    }

    int recieved_bytes = 0;
    while (recieved_bytes < 10) {
      tmp = client.Receive(&clibuff[recieved_bytes], 10);
      ASSERT_GE(tmp, 0);
      recieved_bytes += tmp;
    }
    ASSERT_EQ(string("0123456789"), string(clibuff, 10));
  }
  wait(0);
}

