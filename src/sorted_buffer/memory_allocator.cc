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

#include "src/sorted_buffer/memory_allocator.h"

#include "src/base/common.h"
#include "src/sorted_buffer/memory_piece.h"

namespace sorted_buffer {

//-----------------------------------------------------------------------------
// Implementation of NaiveMemoryAllocator
//-----------------------------------------------------------------------------

NaiveMemoryAllocator::NaiveMemoryAllocator(
    const int pool_size)
    : pool_size_(pool_size),
      allocated_size_(0) {
  CHECK_LT(0, pool_size);
  try {
    pool_ = new char[pool_size];
  } catch(std::bad_alloc&) {
    pool_ = NULL;
    LOG(FATAL) << "Insufficient memory to initialize NaiveMemoryAlloctor with "
               << "pool size = " << pool_size;
  }
}

NaiveMemoryAllocator::~NaiveMemoryAllocator() {
  if (pool_ != NULL) {
    delete [] pool_;
  }
  pool_ = NULL;
  pool_size_ = 0;
  allocated_size_ = 0;
}

bool NaiveMemoryAllocator::Allocate(PieceSize size,
                                    MemoryPiece* piece) {
  CHECK(IsInitialized());
  if (Have(size)) {
    piece->Set(pool_ + allocated_size_, size);
    allocated_size_ += size + sizeof(PieceSize);
    return true;
  }
  piece->Clear();
  return false;
}

bool NaiveMemoryAllocator::Have(PieceSize size) const {
  return size + sizeof(PieceSize) + allocated_size_ <= pool_size_;
}

bool NaiveMemoryAllocator::Have(PieceSize key_length, PieceSize value_length) {
  return allocated_size_ + key_length + value_length + 2 * sizeof(PieceSize)
      <= pool_size_;
}

void NaiveMemoryAllocator::Reset() {
  allocated_size_ = 0;
}

std::ostream& operator<< (std::ostream& output, const MemoryPiece& p) {
  output << "(" << p.Size() << ") ";
  if (p.IsSet()) {
    for (int i = 0; i < p.Size(); ++i) {
      output << p.Data()[i];
    }
  } else {
    output << "[not set]";
  }
  return output;
}

}  // namespace sorted_buffer
