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
// Copyright 2010
// Author: yiwang@tencent.com (Yi Wang)
//
// This file contains facilities that enhance the STL.
//
#ifndef BASE_STL_UTIL_H_
#define BASE_STL_UTIL_H_

// Delete elements (in pointer type) in a STL container like vector,
// list, and deque.
template <class Container>
void STLDeleteElementsAndClear(Container* c) {
  for (typename Container::iterator iter = c->begin();
       iter != c->end(); ++iter) {
    if (*iter != NULL) {
      delete *iter;
    }
  }
  c->clear();
}

// Delete elements (in pointer type) in a STL associative container
// like map and hash_map.
template <class AssocContainer>
void STLDeleteValuesAndClear(AssocContainer* c) {
  for (typename AssocContainer::iterator iter = c->begin();
       iter != c->end(); ++iter) {
    if (iter->second != NULL) {
      delete iter->second;
    }
  }
  c->clear();
}

#endif  // BASE_STL_UTIL_H_
