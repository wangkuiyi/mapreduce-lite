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
#include "src/sorted_buffer/sorted_buffer_iterator.h"

#include <algorithm>
#include "src/base/varint32.h"
#include "src/sorted_buffer/memory_piece.h"
#include "src/sorted_buffer/sorted_buffer.h"

namespace sorted_buffer {

SortedBufferIteratorImpl::SortedBufferIteratorImpl(const std::string& filebase,
                                                   int num_files) {
  Initialize(filebase, num_files);
}

SortedBufferIteratorImpl::~SortedBufferIteratorImpl() {
  Clear();
}

void SortedBufferIteratorImpl::Initialize(const std::string& filebase,
                                          int num_files) {
  CHECK_LE(0, num_files);
  filebase_ = filebase;

  for (int i = 0; i < num_files; ++i) {
    SortedStringFile* file = new SortedStringFile;
    file->index = i;
    file->input =
        fopen(SortedBuffer::SortedFilename(filebase, i).c_str(), "r");
    if (file->input == NULL) {
      LOG(FATAL) << "Cannot open file: "
                 << SortedBuffer::SortedFilename(filebase, i);
    }
    CHECK(LoadKey(file));
    CHECK(LoadValue(file));
    files_.push_back(file);
  }

  heap_size_ = files_.size();
  std::make_heap(files_.begin(), files_.end(), compare_);
  current_key_ = files_[0]->top_key;
  done_ = false;
}

const std::string& SortedBufferIteratorImpl::key() const {
  return current_key_;
}

const std::string& SortedBufferIteratorImpl::value() const {
  return files_[0]->top_value;
}

void SortedBufferIteratorImpl::Next() {
  if (!LoadValue(files_[0])) {
    // top file in heap get to the last value under current key,
    // update the file with next key, and sift down the top file
    if (LoadKey(files_[0])) {
      LoadValue(files_[0]);
      SiftDown();
    } else {
      // top file arrive the end
      std::pop_heap(files_.begin(), files_.begin() + heap_size_, compare_);
      --heap_size_;
    }

    if (heap_size_ == 0 || files_[0]->top_key != current_key_) {
      done_ = true;
    }
  }
}

void SortedBufferIteratorImpl::DiscardRestValues() {
  while (!Done()) {
    Next();
  }
}

bool SortedBufferIteratorImpl::Done() const {
  return done_;
}

void SortedBufferIteratorImpl::NextKey() {
  DiscardRestValues();
  CHECK(Done());
  current_key_ = files_[0]->top_key;
  done_ = false;
}

bool SortedBufferIteratorImpl::FinishedAll() const {
  return heap_size_ == 0;
}

bool SortedBufferIteratorImpl::LoadValue(SortedStringFile* file) {
  if (file->num_rest_values > 0) {
    --(file->num_rest_values);
    if (!ReadMemoryPiece(file->input, &(file->top_value))) {
      LOG(FATAL) << "Error loading value for "
                 << "key = " << file->top_key << " file = "
                 << SortedBuffer::SortedFilename(filebase_, file->index);
    }
    return true;
  }
  return false;
}

bool SortedBufferIteratorImpl::LoadKey(SortedStringFile* file) {
  if (!ReadMemoryPiece(file->input, &(file->top_key))) {
    return false;
  }
  if (!ReadVarint32(file->input,
                    reinterpret_cast<uint32*>(&(file->num_rest_values)))) {
    LOG(FATAL) << "Error load num_rest_values from: "
               << SortedBuffer::SortedFilename(filebase_, file->index);
  }
  if (file->num_rest_values <= 0) {
    LOG(FATAL) << "Zero num_rest_values loaded from "
               << SortedBuffer::SortedFilename(filebase_, file->index);
  }
  return true;
}

void SortedBufferIteratorImpl::SwapFile(int i, int j) {
  SortedStringFile* temp = files_[j];
  files_[j] = files_[i];
  files_[i] = temp;
}

void SortedBufferIteratorImpl::SiftDown() {
  int i = 0;
  int min_child = 0;
  while (i * 2 + 2 < heap_size_) {
    if (compare_(files_[2*i+1], files_[2*i+2])) {
      min_child = 2 * i + 2;  // right child
    } else {
      min_child = 2 * i + 1;  // left child
    }
    if (compare_(files_[i], files_[min_child])) {
      SwapFile(min_child, i);
      i = min_child;
    } else {
      return;
    }
  }

  // current node only has left child
  if (i * 2 + 1 < heap_size_ && compare_(files_[i], files_[i * 2 + 1])) {
    SwapFile(i * 2 + 1, i);
  }
}

void SortedBufferIteratorImpl::Clear() {
  for (SSFileList::iterator i = files_.begin(); i != files_.end(); ++i) {
    fclose((*i)->input);
    delete *i;
  }
  files_.clear();
}

}  // namespace sorted_buffer
