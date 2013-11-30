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
//
#ifndef MAPREDUCE_LITE_SIGNALING_QUEUE_H_
#define MAPREDUCE_LITE_SIGNALING_QUEUE_H_

#include <queue>
#include <set>
#include <string>
#include <utility>  // for pair<>

#include "src/base/common.h"
#include "src/system/condition_variable.h"
#include "src/system/mutex.h"

namespace mapreduce_lite {

// SignalingQueue is a circle queue for using as a buffer in
// the producer/consumer model.  It supports one or more producer
// threads and one or more consumer threads.  Producers invokes Add()
// to push data elements into the queue, and consumers invokes
// Remove() to pop data elements.  Add() and Remove() use two
// condition variables to synchronize producers and consumers.  Each
// producer invokes Signal(producer_id) to claim that it is about to
// finish, where producer_id is an integer uniquely identify a
// producer.  This signaling mechanism prevents consumers from waiting
// after all producers have finished their generation.
//
// SignalingQueue is thread-safe.
//
class SignalingQueue {
 public:
  SignalingQueue(int queue_size /*in bytes*/,
                 int num_producers = 1);
  ~SignalingQueue();

  // return: bytes added to queue
  //  > 0 : size of message
  //  = 0 : not enough space for this message (when is_blocking = false)
  //  - 1 : error
  int Add(const char *src, int size, bool is_blocking = true);
  int Add(const std::string &src, bool is_blocking = true);

  // Remove a message from the queue
  // return: bytes removed from queue
  //  > 0 : size of message
  //  = 0 : queue is empty
  //        invoke NoMoreAdd() to check if all producers have finished
  //  - 1 : fail
  int Remove(char *dest, int max_size, bool is_blocking = true);
  int Remove(std::string *dest, bool is_blocking = true);

  // Signal that producer producer_id will no longer produce anything.
  // After all num_producers_ producers invoked Signal, a special message is
  // then inserted into the queue, so that the consumer can be notified to stop
  // working.
  void Signal(int producer_id);

  // Returns true if queue is empty and all num_producers producers have
  // Signaled their finish.
  bool EmptyAndNoMoreAdd() const;

 private:
  typedef std::pair<int /* message_start_position in queue_ */,
                    int /* message_length */ > MessagePosition;

  char* queue_;          // Pointer to the queue.
  int   queue_size_;     // Size of the queue in bytes.
  int   free_size_;      // Free size in the queue.
  int   write_pointer_;  // Location in queue_ to where write the next element.
                         // Note that we do not need read_pointer since all
                         // messages were indexed by message_positions_, and
                         // the first element in message_positions_ denotes
                         // where we read.
  int   num_producers_;  // Used to check all producers will no longer produce.

  std::queue<MessagePosition> message_positions_;  // Messages in the queue.
  std::set<int /* producer_id */> finished_producers_;

  ConditionVariable cond_not_full_;   // Condition when consumers should wait.
  ConditionVariable cond_not_empty_;  // Condition when producers should wait.
  mutable Mutex     mutex_;           // Protect all above data and conditions.

  DISALLOW_COPY_AND_ASSIGN(SignalingQueue);
};

}  // namespace mapreduce_lite

#endif  // MAPREDUCE_LITE_SIGNALING_QUEUE_H_

