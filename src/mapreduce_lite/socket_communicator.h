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

#ifndef MAPREDUCE_LITE_SOCKET_COMMUNICATOR_H_
#define MAPREDUCE_LITE_SOCKET_COMMUNICATOR_H_

#include <map>
#include <string>
#include <vector>

#include "src/base/scoped_ptr.h"
#include "boost/thread.hpp"
#include "src/mapreduce_lite/communicator.h"
#include "src/mapreduce_lite/signaling_queue.h"
#include "src/mapreduce_lite/tcp_socket.h"

namespace mapreduce_lite {

//-----------------------------------------------------------------------------
// Implement Communicator using TCPSocket:
//-----------------------------------------------------------------------------
class SocketCommunicator : public Communicator {
 public:
  SocketCommunicator() {}
  virtual ~SocketCommunicator() {}

  virtual bool Initialize(bool is_map_worker,
                          int num_map_workers,
                          const std::vector<std::string> &reduce_workers,
                          uint32 map_queue_size /*in bytes*/,
                          uint32 reduce_queue_size /*in bytes*/,
                          int worker_id /*zero-based*/);
  virtual int Send(void *src,
                   int size,
                   int receiver_id /*zero-based*/);
  virtual int Send(const std::string &src,
                   int receiver_id /*zero-based*/);
  virtual int Receive(void *dest, int max_size);
  virtual int Receive(std::string *dest);
  virtual bool Finalize();

 private:

  bool is_sender_;
  int num_sender_;
  int num_receiver_;
  int worker_id_;
  uint32 map_queue_size_;
  uint32 reduce_queue_size_;

  std::vector<TCPSocket*> sockets_;
  std::vector<SignalingQueue*> send_buffers_;
  scoped_ptr<SignalingQueue> receive_buffer_;

  // socket_id_ stores the map from active socket to id
  std::map<int /*socket*/, int /*worker id*/> socket_id_;

  scoped_ptr<boost::thread> thread_send_;
  scoped_ptr<boost::thread> thread_receive_;

  bool InitSender(const std::vector<std::string> &reducers);
  bool InitReceiver(const std::string &reducer);
  bool FinalizeSender();
  bool FinalizeReceiver();

  static void SendLoop(SocketCommunicator *pcom);
  static void ReceiveLoop(SocketCommunicator *pcom);

  DISALLOW_COPY_AND_ASSIGN(SocketCommunicator);
};

}  // namespace mapreduce_lite

#endif  // MAPREDUCE_LITE_SOCKET_COMMUNICATOR_H_
