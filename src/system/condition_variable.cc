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
// Author: Yi Wang (yiwang@tencent.com)
//
// This is a copy from
//   http://tc-svn.tencent.com/setech/setech_infrastructure_rep/Infra_proj/
//   trunk/src/common/system/concurrency/condition_variable.cpp
//
#include "src/system/condition_variable.h"

#include <assert.h>
#if defined __unix__ || defined __APPLE__
#include <sys/time.h>
#endif

#include <stdexcept>
#include <string>


#ifdef _WIN32

ConditionVariable::ConditionVariable() {
  m_hCondition = ::CreateEvent(NULL, FALSE, FALSE, NULL);
  m_nWaitCount = 0;
  assert(m_hCondition != NULL);
}

ConditionVariable::~ConditionVariable() {
  ::CloseHandle(m_hCondition);
}

void ConditionVariable::Wait(Mutex* inMutex) {
  inMutex->Unlock();
  m_nWaitCount++;
  DWORD theErr = ::WaitForSingleObject(m_hCondition, INFINITE);
  m_nWaitCount--;
  assert((theErr == WAIT_OBJECT_0) || (theErr == WAIT_TIMEOUT));
  inMutex->Lock();

  if (theErr != WAIT_OBJECT_0)
    throw std::runtime_error("ConditionVariable::Wait");
}

bool ConditionVariable::Wait(Mutex* inMutex, int inTimeoutInMilSecs) {
  inMutex->Unlock();
  m_nWaitCount++;
  DWORD theErr = ::WaitForSingleObject(m_hCondition, inTimeoutInMilSecs);
  m_nWaitCount--;
  assert((theErr == WAIT_OBJECT_0) || (theErr == WAIT_TIMEOUT));
  inMutex->Lock();

  if (theErr == WAIT_OBJECT_0)
    return true;
  else if (theErr == WAIT_TIMEOUT)
    return false;
  else
    throw std::runtime_error("ConditionVariable::Wait");
}

void ConditionVariable::Signal() {
  if (!::SetEvent(m_hCondition))
    throw std::runtime_error("ConditionVariable::Signal");
}

void ConditionVariable::Broadcast() {
  // There doesn't seem like any more elegant way to
  // implement Broadcast using events in Win32.
  // This will work, it may generate spurious wakeups,
  // but condition variables are allowed to generate
  // spurious wakeups
  unsigned int waitCount = m_nWaitCount;
  for (unsigned int x = 0; x < waitCount; x++) {
    if (!::SetEvent(m_hCondition))
      throw std::runtime_error("ConditionVariable::Broadcast");
  }
}

#elif defined __unix__ || defined __APPLE__

void ConditionVariable::CheckError(const char* context, int error) {
  if (error != 0) {
    std::string msg = context;
    msg += " error: ";
    msg += strerror(error);
    throw std::runtime_error(msg);
  }
}

ConditionVariable::ConditionVariable() {
  pthread_condattr_t cond_attr;
  pthread_condattr_init(&cond_attr);
  int ret = pthread_cond_init(&m_hCondition, &cond_attr);
  pthread_condattr_destroy(&cond_attr);
  CheckError("ConditionVariable::ConditionVariable", ret);
}

ConditionVariable::~ConditionVariable() {
  pthread_cond_destroy(&m_hCondition);
}

void ConditionVariable::Signal() {
  CheckError("ConditionVariable::Signal",
             pthread_cond_signal(&m_hCondition));
}

void ConditionVariable::Broadcast() {
  CheckError("ConditionVariable::Broadcast",
             pthread_cond_broadcast(&m_hCondition));
}

void ConditionVariable::Wait(Mutex* inMutex) {
  CheckError("ConditionVariable::Wait",
             pthread_cond_wait(&m_hCondition, &inMutex->m_Mutex));
}

bool ConditionVariable::Wait(Mutex* inMutex, int inTimeoutInMilSecs) {
  if (inTimeoutInMilSecs < 0) {
    Wait(inMutex);  // wait forever
    return true;
  }

  // get current absolate time
  struct timeval tv;
  gettimeofday(&tv, NULL);

  // add timeout
  tv.tv_sec += inTimeoutInMilSecs / 1000;
  tv.tv_usec += (inTimeoutInMilSecs % 1000) * 1000;

  int million = 1000000;
  if (tv.tv_usec >= million) {
    tv.tv_sec += tv.tv_usec / million;
    tv.tv_usec %= million;
  }

  // convert timeval to timespec
  struct timespec ts;
  ts.tv_sec = tv.tv_sec;
  ts.tv_nsec = tv.tv_usec * 1000;
  int error = pthread_cond_timedwait(&m_hCondition, &inMutex->m_Mutex, &ts);

  if (error == ETIMEDOUT)
    return false;
  else
    CheckError("ConditionVariable::Wait", error);
  return true;
}

#endif

