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
#include <algorithm>

#include "src/sorted_buffer/memory_piece.h"

#include "src/base/common.h"
#include "src/base/varint32.h"

namespace sorted_buffer {

bool MemoryPieceLessThan::operator() (const MemoryPiece& x,
                                      const MemoryPiece& y) const {
  typedef unsigned char byte;
  const byte* xdata = reinterpret_cast<const byte*>(x.Data());
  const byte* ydata = reinterpret_cast<const byte*>(y.Data());
  for (int i = 0; i < std::min(x.Size(), y.Size()); ++i) {
    if (xdata[i] < ydata[i]) {
      return true;
    } else if (xdata[i] > ydata[i]) {
      return false;
    }
  }
  return x.Size() < y.Size();
}

bool MemoryPieceEqual(const MemoryPiece& x, const MemoryPiece& y) {
  typedef unsigned char byte;
  const byte* xdata = reinterpret_cast<const byte*>(x.Data());
  const byte* ydata = reinterpret_cast<const byte*>(y.Data());
  for (int i = 0; i < std::min(x.Size(), y.Size()); ++i) {
    if (xdata[i] != ydata[i]) {
      return false;
    }
  }
  return x.Size() == y.Size();
}

bool WriteMemoryPiece(FILE* output, const MemoryPiece& piece) {
  CHECK(piece.IsSet());
  return WriteVarint32(output, piece.Size()) &&
      ((piece.Size() > 0) ?
       (fwrite(piece.Data(), 1, piece.Size(), output) == piece.Size()) :
       true);
}

bool ReadMemoryPiece(FILE* input, std::string* piece) {
  PieceSize size;
  if (!ReadVarint32(input, &size)) {
    return false;
  }
  // TODO(rickjin): make this re-entrant
  static const int kMaxBufferSize = 32 * 1024 * 1024;
  static char buffer[kMaxBufferSize];
  if (size >= kMaxBufferSize) {
    LOG(FATAL) << "The size of string exceeds kMaxBufferSize "
               << kMaxBufferSize;
  }
  piece->resize(size);
  if (size > 0) {
    if (fread(buffer, 1, size, input) < size) {
      return false;
    }
    piece->assign(buffer, size);
  }
  return true;
}

}  // namespace sorted_buffer


