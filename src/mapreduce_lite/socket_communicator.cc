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

#include "src/mapreduce_lite/socket_communicator.h"

#include <algorithm>  // min()

#include "event2/event.h"
#include "src/base/stl-util.h"
#include "src/strutil/split_string.h"

namespace mapreduce_lite {

using boost::thread;
using std::string;
using std::vector;

//-----------------------------------------------------------------------------
// Connector is used by SocketCommunicator.
// A Connector is used for connecting a TCPSocket to a SignalingQueue.
//  1. Send() reads messages from SignalingQueue, serializes these messages
//     to byte stream, and send it through TCPSocket
//  2. Receive() receives byte stream from TCPSocket, convert it back to
//     messages, and write them to SignalingQueue
//-----------------------------------------------------------------------------
class Connector {
 public:
  Connector();
  void Initialize(SignalingQueue *queue, TCPSocket *sock);

  // return:
  //  1: success
  //  0: queue_ is empty and no more
  //  -1: error
  int Send();

  // return:
  //  1: success
  //  0: no more messages
  //  -1: error
  int Receive();

 private:
  void Clear();
  int SendFinal();        // send a FINAL message

  SignalingQueue *queue_;
  TCPSocket *sock_;
  std::string message_;
  char message_buf_[4096];
  uint32 message_size_;   // size of this message
  uint32 bytes_count_;    // bytes have been sent/received
};

//-----------------------------------------------------------------------------
// Implementation of SocketCommunicator
//-----------------------------------------------------------------------------
bool SocketCommunicator::Initialize(bool is_map_worker,
                                    int  num_map_workers,
                                    const vector<string> &reduce_workers,
                                    uint32 map_queue_size /*in bytes*/,
                                    uint32 reduce_queue_size /*in bytes*/,
                                    int worker_id /*zero-based*/) {
  is_sender_ = is_map_worker;
  num_sender_ = num_map_workers;
  num_receiver_ = reduce_workers.size();
  map_queue_size_ = map_queue_size;
  reduce_queue_size_ = reduce_queue_size;
  worker_id_ = worker_id;

  CHECK_LT(0, map_queue_size_);
  CHECK_LT(0, reduce_queue_size_);
  CHECK_LT(0, num_sender_);
  CHECK_LT(0, num_receiver_);
  if (is_sender_) {
    CHECK_LT(worker_id_, num_sender_);
    return InitSender(reduce_workers);
  } else {
    CHECK_LT(worker_id_, num_receiver_);
    return InitReceiver(reduce_workers[worker_id_]);
  }
}

int SocketCommunicator::Send(void *src, int size,
                             int receiver_id /*zero-based*/) {
  return send_buffers_[receiver_id]->Add(reinterpret_cast<char*>(src), size);
}

int SocketCommunicator::Send(const string &src,
                             int receiver_id /*zero-based*/) {
  return send_buffers_[receiver_id]->Add(src);
}

int SocketCommunicator::Receive(void *dest, int max_size) {
  return receive_buffer_->Remove(reinterpret_cast<char*>(dest), max_size);
}

int SocketCommunicator::Receive(string *dest) {
  return receive_buffer_->Remove(dest);
}

bool SocketCommunicator::Finalize() {
  if (is_sender_) {
    return FinalizeSender();
  } else {
    return FinalizeReceiver();
  }
}

bool SocketCommunicator::InitSender(const vector<string> &reducers) {
  sockets_.resize(num_receiver_);
  send_buffers_.resize(num_receiver_);

  vector<string> ip_and_port;
  try {
    for (int i = 0; i < num_receiver_; ++i) {
      sockets_[i] = new TCPSocket();
      ip_and_port.clear();
      SplitStringUsing(reducers[i], ":", &ip_and_port);
      CHECK_EQ(2, ip_and_port.size());
      CHECK(sockets_[i]->Connect(ip_and_port[0].c_str(),
                                 atoi(ip_and_port[1].c_str())));
      LOG(INFO) << "Sender " << worker_id_
                << " connected to " << reducers[i];

      sockets_[i]->SetBlocking(false);
      socket_id_[sockets_[i]->Socket()] = i;
      send_buffers_[i] = new SignalingQueue(map_queue_size_);
    }
    thread_send_.reset(new thread(SendLoop, this));
  } catch(std::bad_alloc&) {
    LOG(ERROR) << "Cannot allocate memory for sender";
    return false;
  }

  return true;
}

bool SocketCommunicator::InitReceiver(const string &reducer) {
  TCPSocket server;
  vector<string> ip_and_port;
  SplitStringUsing(reducer, ":", &ip_and_port);
  CHECK_EQ(2, ip_and_port.size());
  CHECK(server.Bind(ip_and_port[0].c_str(),
                    atoi(ip_and_port[1].c_str())));
  CHECK(server.Listen(1024));

  string accept_ip;
  uint16 accept_port;
  sockets_.resize(num_sender_);
  try {
    for (int i = 0; i < num_sender_; ++i) {
      sockets_[i] = new TCPSocket();
      CHECK(server.Accept(sockets_[i], &accept_ip, &accept_port));
      LOG(INFO) << "receiver " << worker_id_
                << " accepted from " << accept_ip << ":" << accept_port;
      sockets_[i]->SetBlocking(false);
      socket_id_[sockets_[i]->Socket()] = i;
    }
    receive_buffer_.reset(new SignalingQueue(reduce_queue_size_));
    thread_receive_.reset(new thread(ReceiveLoop, this));
  } catch(std::bad_alloc&) {
    LOG(ERROR) << "Cannot allocate memory for receiver";
    return false;
  }

  return true;
}

bool SocketCommunicator::FinalizeSender() {
  for (int i = 0; i < num_receiver_; ++i) {
    CHECK_NOTNULL(send_buffers_[i]);
    send_buffers_[i]->Signal(worker_id_);
  }

  thread_send_->join();

  STLDeleteElementsAndClear(&sockets_);
  STLDeleteElementsAndClear(&send_buffers_);

  return true;
}

bool SocketCommunicator::FinalizeReceiver() {
  thread_receive_->join();

  STLDeleteElementsAndClear(&sockets_);
  receive_buffer_.reset(NULL);

  return true;
}

struct CallbackArg {
  Connector *connector;
  int *connection_counter;
  struct event_base *base;
};

void SendCallback(evutil_socket_t fd,
                  short event_type /* type of event */,
                  void *arg) {
  CallbackArg *cb_arg = static_cast<CallbackArg*>(arg);
  Connector *con = cb_arg->connector;
  int retval = con->Send();
  CHECK_LE(0, retval);
  if (0 == retval) {
    --*(cb_arg->connection_counter);
  }

  if (*(cb_arg->connection_counter) == 0) {
    event_base_loopexit(cb_arg->base, NULL);
  }
}

/*static*/
void SocketCommunicator::SendLoop(SocketCommunicator *comm) {
  int connection_counter = comm->num_receiver_;
  struct event_base *base = event_base_new();
  vector<struct event *> events(comm->num_receiver_);
  vector<struct CallbackArg *> cb_args(comm->num_receiver_);

  vector<Connector> connectors;
  connectors.resize(comm->num_receiver_);
  for (int i = 0; i < comm->num_receiver_; ++i) {
    connectors[i].Initialize(comm->send_buffers_[i], comm->sockets_[i]);
    cb_args[i] = new(CallbackArg);
    cb_args[i]->connector = &connectors[i];
    cb_args[i]->connection_counter = &connection_counter;
    cb_args[i]->base = base;
    events[i] = event_new(base, comm->sockets_[i]->Socket(),
                          EV_WRITE | EV_PERSIST , SendCallback, cb_args[i]);
    event_add(events[i], NULL);
  }

  event_base_dispatch(base);

  for (int i = 0; i < comm->num_receiver_; ++i) {
    comm->sockets_[i]->ShutDown(SHUT_WR);
    delete(cb_args[i]);
    event_free(events[i]);
  }
  event_base_free(base);
}

void ReceiveCallback(evutil_socket_t fd,
                     short event_type /* type of event */,
                     void *arg) {
  CallbackArg *cb_arg = static_cast<CallbackArg*>(arg);
  Connector *con = cb_arg->connector;
  int retval = con->Receive();
  CHECK_LE(0, retval);
  if (0 == retval) {
    --*(cb_arg->connection_counter);
  }

  if (*(cb_arg->connection_counter) == 0) {
    event_base_loopexit(cb_arg->base, NULL);
  }
}

/*static*/
void SocketCommunicator::ReceiveLoop(SocketCommunicator *comm) {
  int connection_counter = comm->num_sender_;
  struct event_base *base = event_base_new();
  vector<struct event *> events(comm->num_sender_);
  vector<struct CallbackArg *> cb_args(comm->num_receiver_);

  vector<Connector> connectors;
  connectors.resize(comm->num_sender_);
  for (int i = 0; i < comm->num_sender_; ++i) {
    connectors[i].Initialize(comm->receive_buffer_.get(), comm->sockets_[i]);
    cb_args[i] = new(CallbackArg);
    cb_args[i]->connector = &connectors[i];
    cb_args[i]->connection_counter = &connection_counter;
    cb_args[i]->base = base;
    events[i] = event_new(base, comm->sockets_[i]->Socket(),
                          EV_READ | EV_PERSIST , ReceiveCallback, cb_args[i]);
    event_add(events[i], NULL);
  }

  event_base_dispatch(base);

  for (int i = 0; i < comm->num_receiver_; ++i) {
    comm->sockets_[i]->Close();
    delete(cb_args[i]);
    event_free(events[i]);
  }
  event_base_free(base);
  comm->receive_buffer_->Signal(0);
}

//-----------------------------------------------------------------------------
// Implementation of Connector
//-----------------------------------------------------------------------------
Connector::Connector() {
  Clear();
}

void Connector::Initialize(SignalingQueue *queue, TCPSocket *sock) {
  CHECK_NOTNULL(queue);
  CHECK_NOTNULL(sock);
  queue_ = queue;
  sock_ = sock;
}

void Connector::Clear() {
  message_.clear();
  message_size_ = 0;
  bytes_count_ = 0;
}

int Connector::SendFinal() {
  uint32 this_send;
  message_size_ = 0;
  bytes_count_ = 0;

  // send a FINAL message with message_size_ of 0
  while (bytes_count_ < sizeof(message_size_)) {
    this_send = sock_->Send(
        reinterpret_cast<char*>(&message_size_) + bytes_count_,
        sizeof(message_size_) - bytes_count_);
    if (this_send < 0) {
      LOG(ERROR) << "Socket send error.";
      return -1;
    }
    bytes_count_ += this_send;
  }
  return 0;
}

int Connector::Send() {
  // If message_ is empty, read a new message from queue_
  // until the queue_ is empty and will have no more messages
  if (message_.empty()) {
    message_size_ = queue_->Remove(&message_, false);
    if (message_.empty()) {
      if (queue_->EmptyAndNoMoreAdd()) {
        return SendFinal();
      }
      return 1;
    } else {
      message_.insert(0, reinterpret_cast<char*>(&message_size_),
                      sizeof(message_size_));
    }
  }

  // For each message, send message size first, then send the content
  // of this message.  Because sock_->Send() may actually send only
  // part of data in the given buffer, this_send is used to record how
  // many byte have been sent each time, and accumulated to
  // bytes_count_.  So, when (bytes_count_ < sizeof(message_size_)),
  // we continue to send message_size_, else, send message_.data()
  uint32 this_send = 0;
  this_send = sock_->Send(message_.data() + bytes_count_,
                          message_.size() - bytes_count_);
  if (this_send < 0) {
    LOG(ERROR) << "Socket send error.";
    return -1;
  }
  bytes_count_ += this_send;

  // this message complete
  if (bytes_count_ == message_.size()) {
    Clear();
  }

  return 1;
}

int Connector::Receive() {
  // For each message, receive its size first, then receive its content.
  // Similar with Send(), this_receive is used to record how many bytes have
  // been received this time, and accumulated to bytes_count_.
  size_t this_receive = 0;
  if (bytes_count_ >= sizeof(message_size_) && message_size_ == 0) {
    return 0;
  } else {
    this_receive = sock_->Receive(message_buf_, 4096);
  }

  if (this_receive < 0) {
    LOG(ERROR) << "Socket receive error.";
    return -1;
  }

  if (this_receive > 0) {
    size_t pointer = 0;
    size_t len = 0;
    while (pointer < this_receive) {
      if (bytes_count_ < sizeof(message_size_)) {
        len = std::min(sizeof(message_size_) - bytes_count_,
                       this_receive - pointer);
        memcpy(reinterpret_cast<char*>(&message_size_) + bytes_count_,
               message_buf_ + pointer, len);
      } else {
        len = std::min(sizeof(message_size_) + message_size_ - bytes_count_,
                       this_receive - pointer);
        message_.append(message_buf_ + pointer, len);
      }
      bytes_count_ += len;
      pointer += len;
      if (bytes_count_ >= sizeof(message_size_) &&
          message_.size() ==  message_size_) {
        if (message_size_ == 0) return 0;
        if (queue_->Add(message_) < 0) return -1;
        Clear();
      }
    }
  }

  return 1;
}

}  // namespace mapreduce_lite
