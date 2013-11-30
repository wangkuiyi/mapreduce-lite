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
// Author: yiwang@tencent.com (Yi Wang)
//
// This file declares string splitting utilities.
//
#ifndef STRUTIL_SPLIT_STRING_H_
#define STRUTIL_SPLIT_STRING_H_

#include <set>
#include <string>
#include <vector>

// Subdivide string |full| into substrings according to delimitors
// given in |delim|.  |delim| should pointing to a string including
// one or more characters.  Each character is considerred a possible
// delimitor.  For example,
//   vector<string> substrings;
//   SplitStringUsing("apple orange\tbanana", "\t ", &substrings);
// results in three substrings:
//   substrings.size() == 3
//   substrings[0] == "apple"
//   substrings[1] == "orange"
//   substrings[2] == "banana"
void SplitStringUsing(const std::string& full,
                      const char* delim,
                      std::vector<std::string>* result);

// This function has the same semnatic as SplitStringUsing.  Results
// are saved in an STL set container.
void SplitStringToSetUsing(const std::string& full,
                           const char* delim,
                           std::set<std::string>* result);


template <typename T>
struct simple_insert_iterator {
  explicit simple_insert_iterator(T* t) : t_(t) { }

  simple_insert_iterator<T>& operator=(const typename T::value_type& value) {
    t_->insert(value);
    return *this;
  }

  simple_insert_iterator<T>& operator*() { return *this; }
  simple_insert_iterator<T>& operator++() { return *this; }
  simple_insert_iterator<T>& operator++(int placeholder) { return *this; }

  T* t_;
};

template <typename T>
struct back_insert_iterator {
  explicit back_insert_iterator(T& t) : t_(t) {}

  back_insert_iterator<T>& operator=(const typename T::value_type& value) {
    t_.push_back(value);
    return *this;
  }

  back_insert_iterator<T>& operator*() { return *this; }
  back_insert_iterator<T>& operator++() { return *this; }
  back_insert_iterator<T> operator++(int placeholder) { return *this; }

  T& t_;
};


template <typename StringType, typename ITR>
static inline
void SplitStringToIteratorUsing(const StringType& full,
                                const char* delim,
                                ITR& result) {
  // Optimize the common case where delim is a single character.
  if (delim[0] != '\0' && delim[1] == '\0') {
    char c = delim[0];
    const char* p = full.data();
    const char* end = p + full.size();
    while (p != end) {
      if (*p == c) {
        ++p;
      } else {
        const char* start = p;
        while (++p != end && *p != c) {
          // Skip to the next occurence of the delimiter.
        }
        *result++ = StringType(start, p - start);
      }
    }
    return;
  }

  std::string::size_type begin_index, end_index;
  begin_index = full.find_first_not_of(delim);
  while (begin_index != std::string::npos) {
    end_index = full.find_first_of(delim, begin_index);
    if (end_index == std::string::npos) {
      *result++ = full.substr(begin_index);
      return;
    }
    *result++ = full.substr(begin_index, (end_index - begin_index));
    begin_index = full.find_first_not_of(delim, end_index);
  }
}

#endif  // STRUTIL_SPLIT_STRING_H_
