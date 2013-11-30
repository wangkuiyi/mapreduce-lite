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
//   trunk/src/common/system/concurrency/condition_variable.hpp
//
#ifndef SYSTEM_CONDITION_VARIABLE_H_
#define SYSTEM_CONDITION_VARIABLE_H_

#ifndef _WIN32
#if defined __unix__ || defined __APPLE__
#include <pthread.h>
#endif
#endif

#include <assert.h>
#include "src/system/mutex.h"

class ConditionVariable {
 public:
  ConditionVariable();
  ~ConditionVariable();

  void Signal();
  void Broadcast();

  bool Wait(Mutex* inMutex, int inTimeoutInMilSecs);
  void Wait(Mutex* inMutex);

 private:
#if defined _WIN32
  HANDLE m_hCondition;
  unsigned int m_nWaitCount;
#elif defined __unix__ || defined __APPLE__
  pthread_cond_t m_hCondition;
#endif
  static void CheckError(const char* context, int error);
};

#endif  // SYSTEM_CONDITION_VARIABLE_H_

