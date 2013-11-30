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

#ifndef MAPREDUCE_LITE_COMMUNICATOR_H_
#define MAPREDUCE_LITE_COMMUNICATOR_H_

#include <map>
#include <string>
#include <vector>

#include "src/base/common.h"

namespace mapreduce_lite {

//-----------------------------------------------------------------------------
// The interface implemented by ``real'' Communicators.
//-----------------------------------------------------------------------------
class Communicator {
 public:
  virtual ~Communicator() {}

  /* Initialize:
   * get information via command line arguments:
   *  - mr_num_map_workers=5
   *  - mr_reduce_workers="m1:7070,m2:7070,m2:7071,m2:7072,m3:7070,m3:8080"
   *  - mr_map_worker_id=x
   *  - mr_reduce_worker_id=y
   *
   * for reduce workers:
   *  - bind tcp socket and listen on specified port
   *  - wait for Mappers to connect in
   *
   * for map workers:
   *  - connect to all Reducers
   *
   * Return:
   *  - success or not
   */
  virtual bool Initialize(bool is_map_worker,
                          int num_map_workers,
                          const std::vector<std::string> &reduce_workers,
                          uint32 map_queue_size /*in bytes*/,
                          uint32 reduce_queue_size /*in bytes*/,
                          int worker_id /*zero-based*/) = 0;

  /* Send:
   *  - send a message to a specified Reducer
   *  - actually write message into buffer
   * Return:
   *  >0: bytes send
   *  -1: error
   */
  virtual int Send(void *src,
                   int size,
                   int receiver_id /*zero-based*/) = 0;
  virtual int Send(const std::string &src,
                   int receiver_id /*zero-based*/) = 0;

  /* Receive:
   *  - receive a message from any Mapper
   *  - actually read a message out from buffer
   * Return:
   *  > 0: bytes recived
   *  = 0: there will be no more messages in message queue
   *  - 1: error
   */
  virtual int Receive(void *dest, int max_size) = 0;
  virtual int Receive(std::string *dest) = 0;

  /* close all sockets
   *  - Mapper close the sockets
   *  - Reducer will be notified that a socket is closed
   */
  virtual bool Finalize() = 0;
};

}  // namespace mapreduce_lite

#endif  // MAPREDUCE_LITE_COMMUNICATOR_H_
