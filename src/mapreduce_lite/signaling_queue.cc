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

#include "src/mapreduce_lite/signaling_queue.h"

#include <cstring>

namespace mapreduce_lite {

using std::string;

SignalingQueue::SignalingQueue(int queue_size, /* in bytes */
                               int num_producers) {
  CHECK_LT(0, queue_size);
  try {
    queue_ = new char[queue_size];
  } catch(const std::bad_alloc&) {
    LOG(FATAL) << "Not enough memory for message buffer.";
  }
  memset(queue_, '\0', queue_size);

  queue_size_     = queue_size;
  free_size_      = queue_size;
  write_pointer_  = 0;
  num_producers_  = num_producers;
}

SignalingQueue::~SignalingQueue() {
  MutexLocker locker(&mutex_);
  if (NULL != queue_) {
    delete [] queue_;
    queue_ = NULL;
  }
}

int SignalingQueue::Add(const char *src, int size, bool is_blocking) {
  // check if message too long to fit in the queue.
  if (size > queue_size_) {
    LOG(ERROR) << "Message is larger than the queue.";
    return -1;
  }

  if (size <= 0) {
    LOG(ERROR) << "Message size (" << size << ") is negative or zero.";
    return -1;
  }

  MutexLocker locker(&mutex_);
  if (finished_producers_.size() >= num_producers_) {
    LOG(ERROR) << "Can't add to buffer when flag_no_more_ is set";
    return -1;
  }

  while (size > free_size_) {
    if (!is_blocking) {
      return 0;
    }
    cond_not_full_.Wait(&mutex_);
  }

  // write data into buffer:
  // if there is enough space on tail of buffer, just append data
  // else, write till the end of buffer and return to head of buffer
  message_positions_.push(std::make_pair(write_pointer_, size));
  free_size_ -= size;
  if (write_pointer_ + size <= queue_size_) {
    memcpy(&queue_[write_pointer_], src, size);
    write_pointer_ += size;
    if (write_pointer_ == queue_size_)
      write_pointer_ = 0;
  } else {
    int size_partial = queue_size_ - write_pointer_;
    memcpy(&queue_[write_pointer_], src, size_partial);
    memcpy(queue_, &src[size_partial], size - size_partial);
    write_pointer_ = size - size_partial;
  }

  // not empty signal
  cond_not_empty_.Signal();

  return size;
}

int SignalingQueue::Add(const string &src, bool is_blocking) {
  return Add(src.data(), src.size(), is_blocking);
}

int SignalingQueue::Remove(char *dest, int max_size, bool is_blocking) {
  int retval;

  MutexLocker locker(&mutex_);
  while (message_positions_.empty()) {
    if (!is_blocking) {
      return 0;
    }
    if (finished_producers_.size() >= num_producers_) {
      return 0;
    }
    cond_not_empty_.Wait(&mutex_);
  }

  MessagePosition & pos = message_positions_.front();
  // check if message too long
  if (pos.second > max_size) {
    LOG(ERROR) << "Message size exceeds limit, information lost.";
    retval = -1;
  } else {
    // read from buffer:
    // if this message stores in consecutive memory, just read
    // else, read from buffer tail then return to head
    if (pos.first + pos.second <= queue_size_) {
      memcpy(dest, &queue_[pos.first], pos.second);
    } else {
      int size_partial = queue_size_ - pos.first;
      memcpy(dest, &queue_[pos.first], size_partial);
      memcpy(&dest[size_partial], queue_, pos.second - size_partial);
    }
    retval = pos.second;
  }
  free_size_ += pos.second;
  message_positions_.pop();

  cond_not_full_.Signal();

  return retval;
}

int SignalingQueue::Remove(string *dest, bool is_blocking) {
  int retval;

  MutexLocker locker(&mutex_);
  while (message_positions_.empty()) {
    if (!is_blocking) {
      return 0;
    }
    if (finished_producers_.size() >= num_producers_) {
      return 0;
    }
    cond_not_empty_.Wait(&mutex_);
  }

  MessagePosition & pos = message_positions_.front();
  // read from buffer:
  // if this message stores in consecutive memory, just read
  // else, read from buffer tail then return to head
  if (pos.first + pos.second <= queue_size_) {
    dest->assign(&queue_[pos.first], pos.second);
  } else {
    int size_partial = queue_size_ - pos.first;
    dest->assign(&queue_[pos.first], size_partial);
    dest->append(queue_, pos.second - size_partial);
  }
  retval = pos.second;
  free_size_ += pos.second;
  message_positions_.pop();

  cond_not_full_.Signal();

  return retval;
}

void SignalingQueue::Signal(int producer_id) {
  MutexLocker locker(&mutex_);
  finished_producers_.insert(producer_id);

  // if all producers have finished, consumers should be waken up to
  // get this signal
  if (finished_producers_.size() >= num_producers_) {
    cond_not_empty_.Broadcast();
  }
}

bool SignalingQueue::EmptyAndNoMoreAdd() const {
  MutexLocker locker(&mutex_);
  return message_positions_.size() == 0 &&
         finished_producers_.size() >= num_producers_;
}

}  // namespace mapreduce_lite
