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
// MemoryPiece encapsulate either a pointer to a std::string, or a
// pointer to a memory block with size = sizeof(PieceSize) +
// block_size, where the first sizeof(PieceSize) bytes saves the value
// of block_size.  The main purpose of MemoryPiece is to save string
// in raw memory allocated from user-defined memory pool.  The reason
// of encapsulating std::string is to make it possible to compare a
// std::string with a MemoryPiece that encapsulate a memory-block.
//
// MemoryPieceLessThan is a binary comparator for sorting MemoryPieces
// in lexical order.
//
// ReadMemoryPiece and WriteMemoryPiece supports (local) file IO of
// MemoryPieces.
//
#ifndef SORTED_BUFFER_MEMORY_PIECE_H_
#define SORTED_BUFFER_MEMORY_PIECE_H_

#include <stdio.h>
#include <functional>
#include <string>

#include "src/base/common.h"

namespace sorted_buffer {

typedef uint32 PieceSize;


// Represent either a piece of memory, which is prepended by a
// PieceSize, or a std::string object.
class MemoryPiece {
  friend std::ostream& operator<< (std::ostream&, const MemoryPiece& p);

 public:
  MemoryPiece() : piece_(NULL), string_(NULL) {}
  MemoryPiece(char* piece, PieceSize size) { Set(piece, size); }
  explicit MemoryPiece(std::string* string) { Set(string); }

  void Set(char* piece, PieceSize size) {
    CHECK_LE(0, size);
    CHECK_NOTNULL(piece);
    // TODO(charlieyan): fix bug here
    piece_ = piece;
    *reinterpret_cast<PieceSize*>(piece_) = size;
    string_ = NULL;
  }

  void Set(std::string* string) {
    CHECK_NOTNULL(string);
    string_ = string;
    piece_ = NULL;
  }

  void Clear() {
    piece_ = NULL;
    string_ = NULL;
  }

  bool IsSet() const { return IsString() || IsPiece(); }
  bool IsString() const { return string_ != NULL; }
  bool IsPiece() const { return piece_ != NULL; }

  const char* Piece() const { return piece_; }

  char* Data() {
    return IsPiece() ? piece_ + sizeof(PieceSize) :
        (IsString() ? const_cast<char*>(string_->data()) : NULL);
  }

  const char* Data() const {
    return IsPiece() ? piece_ + sizeof(PieceSize) :
        (IsString() ? string_->data() : NULL);
  }

  size_t Size() const {
    return IsPiece() ? *reinterpret_cast<PieceSize*>(piece_) :
        (IsString() ? string_->size() : 0);
  }

 private:
  char* piece_;
  std::string* string_;
};


// Compare two MemoryPiece objects in lexical order.
struct MemoryPieceLessThan : public std::binary_function<const MemoryPiece&,
                                                         const MemoryPiece&,
                                                         bool> {
  bool operator() (const MemoryPiece& x, const MemoryPiece& y) const;
};

bool MemoryPieceEqual(const MemoryPiece& x, const MemoryPiece& y);

bool WriteMemoryPiece(FILE* output, const MemoryPiece& piece);
bool ReadMemoryPiece(FILE* input, std::string* piece);

}  // namespace sorted_buffer

#endif  // SORTED_BUFFER_MEMORY_PIECE_H_
