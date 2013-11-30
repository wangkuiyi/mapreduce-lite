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
// Copyright 2011 Tencent Inc.
// Author: Zhihui Jin (rickjin@tencent.com)
//         Huan Yu (huanyu@tencent.com)
//
// Wrappers for iostream, use stdin and stdout if filename is "-"
//
#ifndef BASE_STREAM_WRAPPER_H_
#define BASE_STREAM_WRAPPER_H_

#include <cstring>
#include <fstream>
#include <iostream>

namespace stream_wrapper {

/**
 * @brief 包装输出流, 使得标准输出和普通文件输出接口一致
 */
class ostream_wrapper
{
 public:
  std::ostream &operator*() const  { return *output_stream_; }
  std::ostream *operator->() const { return output_stream_;  }

  explicit ostream_wrapper(const char* filename);
  virtual ~ostream_wrapper();

 private:
  std::ostream* output_stream_;
};

/**
 * @brief 包装输入流, 使得标准输入和普通文件输入接口一致
 */
class istream_wrapper {
 public:
  std::istream &operator*() const  { return *input_stream_; }
  std::istream *operator->() const { return input_stream_;  }

  explicit istream_wrapper(const char* filename);
  virtual ~istream_wrapper();

 private:
  std::istream* input_stream_;
};

}  // namespace
#endif  // BASE_STREAM_WRAPPER_H_
