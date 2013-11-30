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
// Author: YAN Hao (charlieyan@tencent.com)

#ifndef MAPREDUCE_LITE_TCP_SOCKET_H_
#define MAPREDUCE_LITE_TCP_SOCKET_H_

#include <sys/socket.h>
#include <string>

#include "src/base/common.h"

namespace mapreduce_lite {

// TCPSocket is a simple wrapper around a socket. It supports
// only TCP connections.
class TCPSocket {
 public:
  // ctor and dctor
  TCPSocket();
  ~TCPSocket();

  // Return value of following functions:
  //  true for success and false for failure
  // connect to a given server address
  bool Connect(const char * ip, uint16 port);

  // bind on the given IP ans PORT
  bool Bind(const char * ip, uint16 port);

  // listen
  bool Listen(int max_connection);

  // wait for a new connection
  // new SOCKET, IP and PORT will be stored to socket, ip_client and port_client
  bool Accept(TCPSocket * socket,
              std::string * ip_client,
              uint16 * port_client);

  // SetBlocking() is needed refering to this example of epoll:
  // http://www.kernel.org/doc/man-pages/online/pages/man4/epoll.4.html
  bool SetBlocking(bool flag);

  // Shut down one or both halves of the connection.
  // If ways is SHUT_RD, further receives are disallowed.
  // If ways is SHUT_WR, further sends are disallowed.
  // If ways is SHUT_RDWR, further sends and receives are disallowed.
  bool ShutDown(int ways);

  // close socket
  void Close();

  // send/receive data:
  // return number of bytes read or written if OK, -1 on error
  // caller is responsible for checking that all data has been sent/received,
  // if not, extra send/receive should be invoked
  int Send(const char * data, int len_data);
  int Receive(char * buffer, int size_buffer);

  // return socket's file descriptor
  int Socket() const;

 private:
  int socket_;

  DISALLOW_COPY_AND_ASSIGN(TCPSocket);
};

}  // namespace mapreduce_lite

#endif  // MAPREDUCE_LITE_TCP_SOCKET_H_

