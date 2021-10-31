//
//  Stryke
//
//  https://github.com/edmBernard/Stryke
//
//  Created by Erwan BERNARD on 02/12/2018.
//
//  Copyright (c) 2018, 2019, 2020, 2021 Erwan BERNARD. All rights reserved.
//  Distributed under the Apache License, Version 2.0. (See accompanying
//  file LICENSE or copy at http://www.apache.org/licenses/LICENSE-2.0)
//
#pragma once

#include <cstdint>

namespace stryke {

class WriterOptions {
public:
  WriterOptions() {
  }

  void disable_lock_file(bool disable_lock_file = true) {
    this->create_lock_file = !disable_lock_file;
  }

  void enable_suffix_timestamp(bool enable_suffix_timestamp = true) {
    this->suffix_timestamp = enable_suffix_timestamp;
  }

  void set_batch_size(uint64_t batchSize) {
    this->batchSize = batchSize;
  }

  void set_batch_max(int nbr_batch_max) {
    this->nbr_batch_max = nbr_batch_max;
  }

  void set_stripe_size(uint64_t stripeSize) {
    this->stripeSize = stripeSize;
  }

  void set_cron(int minutes) {
    this->cron = minutes;
  }

  bool create_lock_file = true;
  bool suffix_timestamp = false;
  uint64_t batchSize = 10000;
  int nbr_batch_max = 0;
  uint64_t stripeSize = 10000;
  int cron = -1;
};

} // namespace stryke
