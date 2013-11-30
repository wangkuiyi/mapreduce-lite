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

#include "src/system/filepattern.h"

#include "src/base/common.h"

FilepatternMatcher::FilepatternMatcher(const std::string& filepattern) {
  glob_return_ = glob(filepattern.c_str(),
                      GLOB_MARK |        // Append a slash to each path which
                                         // corresponds to a directory.
#if defined GLOB_TILDE_CHECK && defined GLOB_BRACE
                      GLOB_TILDE_CHECK | // Carry out tilde
                                         // expansion.If the username
                                         // is invalid, or the home
                                         // directory cannot be
                                         // determined, glob() returns
                                         // GLOB_NOMATCH to indicate
                                         // an error.
                      GLOB_BRACE,        // Enable brace expressions.
#else
                      0,  // On neither Cygwin, BSD nor Drawin,
                          // GLOB_TILDE_CHECK and GLOB_BRACE are not
                          // defined.
#endif
                      FilepatternMatcher::ErrorFunc,
                      &glob_result_);

  if (glob_return_ != 0) {
    switch (glob_return_) {
      case GLOB_NOSPACE:
        LOG(ERROR) << "Run out of memory in file pattern matching.";
        break;
      case GLOB_ABORTED:
        LOG(ERROR) << "Encouterred read error in file pattern matching.";
        break;
      case GLOB_NOMATCH:
        LOG(ERROR) << "Filepattern " << filepattern << " matches no file.";
        break;
    }
  }
}

bool FilepatternMatcher::NoError() const {
  return glob_return_ == 0;
}

int FilepatternMatcher::NumMatched() const {
  return glob_result_.gl_pathc;
}

const char* FilepatternMatcher::Matched(int i) const {
  CHECK_LE(0, i);
  CHECK_LT(i, NumMatched());
  return glob_result_.gl_pathv[i];
}

FilepatternMatcher::~FilepatternMatcher() {
  globfree(&glob_result_);
}

/*static*/
int FilepatternMatcher::ErrorFunc(const char* epath, int eerrno) {
  LOG(ERROR) << "Failed at matching: " << epath << " due to error: " << eerrno;
  return 0;  // Retuns 0 to let glob() attempts carry on despite errors.
}
