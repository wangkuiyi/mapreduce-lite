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
// Define class templates CVector<Element> and CVector<Element*>
//
// CVector<Element> is designed to encapsulate C-style vectors
// (including malloc, free and memcpy-type initialization) with a
// minimum interface.  Compared with std::vector, CVector ensures that
// elements are stored successively in memory layout, and provides an
// interface to access the elements in C-style.
//
// CVector<Element*> is a partial specialization of CVector<Element>.
// Whenever you assign a pointer to any element in CVector<Element*>,
// CVector<Element*> takes the ownership of your pointer, and will
// free it in CVector<Element*>'s destructure using C++ keyword delete.
//
// Example:
//
// Many C libraries like GNU Scientific Library (GSL) operates C-style
// vectors.  The following example shows how to draw a sample
// (parameters of a multinomial distribution) from a symmetric
// Dirichlet distribution:
/*
  const int     kDim = 10;
  const double  kParam = 0.1
  CVector<double> dirichlet_param(kDim, kParam);
  CVector<double> multinomial_param(kDim);
  gsl_ran_dirichlet(gsl_rng,
  dirichlet_param.size(), dirichlet_param.data(),
  multinomial_param.data());
*/
//
// Using CVector<Element*> and CVector<Element>, it is easy to define
// a matrix (2D array).  For example:
/*
  template <typename Element>
  class Matrix {
  public:
  void resize(int n, int m) {
  rows_.resize(n);
  for (int i = 0; i < n; ++i) {
  rows_.data()[i] = new CVector<Element>(m, 0);
  }
  }
  private:
  CVector<Element*> rows_;
  };
*/

#ifndef BASE_CVECTOR_H_
#define BASE_CVECTOR_H_

#include <algorithm>

#include "src/base/common.h"

template <typename Element>
class CVector {
 public:
  CVector(int size, const Element& init) {
    Allocate(size);
    Initialize(init);
  }

  explicit CVector(int size) { Allocate(size); }

  CVector() {
    data_ = NULL;
    size_ = 0;
  }

  ~CVector() { Deallocate(); }

  void resize(int size, const Element& init) {
    Deallocate();
    Allocate(size);
    Initialize(init);
  }

  void resize(int size) {
    Deallocate();
    Allocate(size);
  }

  int size() const { return size_; }

  const Element* data() const { return data_; }
  Element*       data()       { return data_; }

 protected:
  void Allocate(int size) {
    size_ = size;
    data_ = new Element[size];
  }

  void Deallocate() {
    delete [] data_;
    data_ = NULL;
  }

  void Initialize(const Element& init) {
    for (int i = 0; i < size_; ++i) {
      data_[i] = init;
    }
  }

  Element* data_;
  int      size_;

 private:
  DISALLOW_COPY_AND_ASSIGN(CVector);
};


template <typename Element>
class CVector<Element*> {
 public:
  explicit CVector(int size) {
    Allocate(size);
    Initialize(NULL);
  }

  CVector() {
    data_ = NULL;
    size_ = 0;
  }

  ~CVector() { Deallocate(); }

  void resize(int size, const Element* init) {
    Deallocate();
    Allocate(size);
    Initialize(init);
  }

  void resize(int size) {
    Deallocate();
    Allocate(size);
  }

  int size() const { return size_; }

  const Element** data() const { return data_; }
  Element**       data()       { return data_; }

 protected:
  void Allocate(int size) {
    size_ = size;
    data_ = new Element*[size];
  }

  void Deallocate() {
    for (int i = 0; i < size_; ++i) {
      if (data_[i] != NULL) {
        delete data_[i];
      }
    }
    delete [] data_;
    data_ = NULL;
  }

  void Initialize(Element* init) {
    for (int i = 0; i < size_; ++i) {
      data_[i] = init;
    }
  }

  Element** data_;
  int      size_;

 private:
  DISALLOW_COPY_AND_ASSIGN(CVector);
};

#endif  // BASE_CVECTOR_H_

