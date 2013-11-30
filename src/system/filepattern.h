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
// Author: Yi Wang (yiwang@tencent.com)
//
// Operations with file patterns.
//
#ifndef SYSTEM_FILEPATTERN_H_
#define SYSTEM_FILEPATTERN_H_

#include <glob.h>
#include <string>

// Given a filepattern as a string, this class find mached files.
// Usage:
/*
  const string filepattern = "/usr/include/g*.h";
  FilepatternMatcher m(filepattern);
  for (int i = 0; i < m.NumMatched(); ++i) {
    printf("Found %s\n", m.Matched());
  }
*/
class FilepatternMatcher {
 public:
  explicit FilepatternMatcher(const std::string& filepattern);
  ~FilepatternMatcher();

  bool NoError() const;
  int NumMatched() const;
  const char* Matched(int i) const;

 private:
  static int ErrorFunc(const char* epath, int eerrno);  // The error handler.
  int glob_return_;
  glob_t glob_result_;
};

#endif  // SYSTEM_FILEPATTERN_H_
