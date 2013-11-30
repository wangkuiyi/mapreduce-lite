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
//         Zhihui Jin (rickjin@tencent.com)
//
// Wrappers for several pseudo-random number generators used in LDA, including
// the default unix RNG(random number generator), and RNGs provided in Boost
// library.  We suggest not using unix RNG due to its poor randomness in
// application.
//
#ifndef BASE_RANDOM_H_
#define BASE_RANDOM_H_

#include "src/base/common.h"
#include "boost/random.hpp"

// The RNG wrapper interface
class Random {
 public:
  Random() {}

  virtual ~Random() {}

  // Seed the RNG using specified seed or current time(if seed < 0).
  // In order to achieve maximum randomness we use current time in
  // millisecond as the seed.  Note that it is not a good idea to
  // seed with current time in second when multiple random number
  // sequences are required, which usually produces correlated number
  // sequences and results in poor randomness.
  virtual void SeedRNG(int seed) = 0;

  // Generate a random float value in the range of [0,1) from the
  // uniform distribution.
  virtual double RandDouble() = 0;

  // Generate a random integer value in the range of [0,bound) from the
  // uniform distribution.
  virtual int RandInt(int bound) {
    return static_cast<int>(RandDouble() * bound);
  }

  // Get tick count of the day, used as random seed
  static uint32 GetTickCount();
};


// Wrapper for default C-runtime random number generator
class CRuntimeRandom : public Random {
 public:
  CRuntimeRandom() { seed_ = 0; }

  virtual ~CRuntimeRandom() {}

  virtual void SeedRNG(int seed);

  virtual double RandDouble() {
    // rand() returns a pseudo-random integral number in the range
    // [0,RAND_MAX].  original code will generate a random float in [0,1], not
    // [0, 1) WARNING : RAND_MAX is the largest integer, so we should cast it
    // to double before we do RADND_MAX+1
    return rand_r(&seed_) / (static_cast<double>(RAND_MAX) + 1);
  }

 private:
  unsigned int seed_;
};


// The RNG(random number generator) wrapper using Boost mt19937.
// Please refer to [http://www.boost.org/doc/libs/1_44_0/doc/html/
// boost_random/reference.html#boost_random.reference.generators]
// for details about mt19937 generator and uniform_01 distribution.
class MTRandom : public Random {
 public:
  MTRandom()
      : uniform_01_rng_(boost::mt19937()) {}

  virtual ~MTRandom() {}

  virtual void SeedRNG(int seed);

  virtual double RandDouble() {
    return uniform_01_rng_();
  }

 private:
  boost::uniform_01<boost::mt19937>  uniform_01_rng_;
};

#endif  // BASE_RANDOM_H_
