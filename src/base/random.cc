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
#include "src/base/random.h"

#include <stdlib.h>
#include <sys/time.h>
#include <time.h>
#include <unistd.h>

/*static*/
uint32 Random::GetTickCount() {
  struct timeval t;
  gettimeofday(&t, NULL);
  t.tv_sec %= (24 * 60 * 60);  // one day ticks 24*60*60
  uint32 tick_count = t.tv_sec * 1000 + t.tv_usec / 1000;
  return tick_count;
}

void CRuntimeRandom::SeedRNG(int seed) {
  if (seed >= 0) {
    seed_ = seed;
  } else {
    seed_ = getpid() * time(NULL);  // BUG(yiwang): should also times thread id
  }
}

void MTRandom::SeedRNG(int seed) {
  if (seed >= 0) {
    uniform_01_rng_.base().seed(seed);
  } else {
    uniform_01_rng_.base().seed(GetTickCount());
  }
}

