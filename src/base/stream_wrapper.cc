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
#include "src/base/stream_wrapper.h"

#include <cstring>
#include <fstream>
#include <iostream>

namespace stream_wrapper {

ostream_wrapper::ostream_wrapper(const char* filename)
    : output_stream_(0) {
  if (std::strcmp(filename, "-") == 0)
    output_stream_ = &std::cout;
  else
    output_stream_ = new std::ofstream(filename);
}

ostream_wrapper::~ostream_wrapper() {
  if (output_stream_ != &std::cout) delete output_stream_;
}

istream_wrapper::istream_wrapper(const char* filename)
    : input_stream_(0) {
  if (std::strcmp(filename, "-") == 0)
    input_stream_ = &std::cin;
  else
    input_stream_ = new std::ifstream(filename, std::ios::binary|std::ios::in);
}

istream_wrapper::~istream_wrapper() {
  if (input_stream_ != &std::cin) delete input_stream_;
}

}
