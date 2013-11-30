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

#include "src/mapreduce_lite/tcp_socket.h"

#include <arpa/inet.h>
#include <fcntl.h>
#include <netdb.h>
#include <netinet/in.h>
#include <pthread.h>
#include <sys/socket.h>
#include <unistd.h>

namespace mapreduce_lite {

typedef struct sockaddr_in SAI;
typedef struct sockaddr SA;

// ctor
TCPSocket::TCPSocket() {
  // init socket
  socket_ = socket(AF_INET, SOCK_STREAM, 0);
  if (socket_ < 0) {
    LOG(FATAL) << "Can't create new socket.";
  }
}

// dctor
TCPSocket::~TCPSocket() {
  Close();
}

bool TCPSocket::Connect(const char * ip, uint16 port) {
  SAI sa_server;
  sa_server.sin_family      = AF_INET;
  sa_server.sin_port        = htons(port);

  if (0 < inet_pton(AF_INET, ip, &sa_server.sin_addr) &&
      0 <= connect(socket_, reinterpret_cast<SA*>(&sa_server),
                   sizeof(sa_server))) {
    return true;
  }

  LOG(ERROR) << "Failed connect to " << ip << ":" << port;
  return false;
}

bool TCPSocket::Bind(const char * ip, uint16 port) {
  SAI sa_server;
  sa_server.sin_family      = AF_INET;
  sa_server.sin_port        = htons(port);

  if (0 < inet_pton(AF_INET, ip, &sa_server.sin_addr) &&
      0 <= bind(socket_, reinterpret_cast<SA*>(&sa_server),
                sizeof(sa_server))) {
    return true;
  }

  LOG(ERROR) << "Failed bind on " << ip << ":" << port;
  return false;
}

bool TCPSocket::Listen(int max_connection) {
  if (0 <= listen(socket_, max_connection)) {
    return true;
  }

  LOG(ERROR) << "Failed listen on socket fd: " << socket_;
  return false;
}

bool TCPSocket::Accept(TCPSocket * socket, std::string * ip, uint16 * port) {
  int sock_client;
  SAI sa_client;
  socklen_t len = sizeof(sa_client);

  sock_client = accept(socket_, reinterpret_cast<SA*>(&sa_client), &len);
  if (sock_client < 0) {
    LOG(ERROR) << "Failed accept connection on " << ip << ":" << port;
    return false;
  }

  char tmp[INET_ADDRSTRLEN];
  const char * ip_client = inet_ntop(AF_INET,
                                     &sa_client.sin_addr,
                                     tmp,
                                     sizeof(tmp));
  CHECK_NOTNULL(ip_client);
  ip->assign(ip_client);
  *port = ntohs(sa_client.sin_port);
  socket->socket_ = sock_client;

  return true;
}

bool TCPSocket::SetBlocking(bool flag) {
  int opts;

  if ((opts = fcntl(socket_, F_GETFL)) < 0) {
    LOG(ERROR) << "Failed to get socket status.";
    return false;
  }

  if (flag) {
    opts |= O_NONBLOCK;
  } else {
    opts &= ~O_NONBLOCK;
  }

  if (fcntl(socket_, F_SETFL, opts) < 0) {
    LOG(ERROR) << "Failed to set socket status.";
    return false;
  }

  return true;
}

bool TCPSocket::ShutDown(int ways) {
  return 0 == shutdown(socket_, ways);
}

void TCPSocket::Close() {
  if (socket_ >= 0) {
    CHECK_EQ(0, close(socket_));
    socket_ = -1;
  }
}

int TCPSocket::Send(const char * data, int len_data) {
  return send(socket_, data, len_data, 0);
}

int TCPSocket::Receive(char * buffer, int size_buffer) {
  return recv(socket_, buffer, size_buffer, 0);
}

int TCPSocket::Socket() const {
  return socket_;
}

}  // namespace mapreduce_lite
