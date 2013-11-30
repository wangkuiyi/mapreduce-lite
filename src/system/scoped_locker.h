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
// This implementation is a copy from
//   http://tc-svn.tencent.com/setech/setech_infrastructure_rep/Infra_proj/
//   trunk/src/common/system/concurrency/scoped_locker.hpp

#ifndef SYSTEM_SCOPED_LOCKER_H_
#define SYSTEM_SCOPED_LOCKER_H_

#include "src/base/common.h"

template <typename LockType>
class ScopedLocker {
 public:
  explicit ScopedLocker(LockType* lock) : m_lock(lock) {
    m_lock->Lock();
  }
  ~ScopedLocker() {
    m_lock->Unlock();
  }
 private:
  LockType* m_lock;
};

template <typename LockType>
class ScopedReaderLocker {
 public:
  explicit ScopedReaderLocker(LockType* lock) : m_lock(lock) {
    m_lock->ReaderLock();
  }
  ~ScopedReaderLocker() {
    m_lock->ReaderUnlock();
  }
 private:
  LockType* m_lock;
};

template <typename LockType>
class ScopedWriterLocker {
 public:
  explicit ScopedWriterLocker(LockType* lock) : m_lock(*lock) {
    m_lock.WriterLock();
  }
  ~ScopedWriterLocker() {
    m_lock.WriterUnlock();
  }
 private:
  LockType& m_lock;
};

#endif  // SYSTEM_SCOPED_LOCKER_H_
