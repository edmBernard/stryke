//
//  Stryke
//
//  https://github.com/edmBernard/Stryke
//
//  Created by Erwan BERNARD on 08/12/2018.
//
//  Copyright (c) 2018 Erwan BERNARD. All rights reserved.
//  Distributed under the Apache License, Version 2.0. (See accompanying
//  file LICENSE or copy at http://www.apache.org/licenses/LICENSE-2.0)
//
#pragma once
#ifndef STRYKE_THREAD_HPP_
#define STRYKE_THREAD_HPP_

#include "stryke_template.hpp"
#include <exception>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <map>
#include <string>
#include <thread>
#include <vector>

namespace stryke {

namespace fs = std::filesystem;

namespace utils {

} // namespace utils

//! Writer in mutli file one thread.
//!
//!
template <template <typename... Types> typename Writer, typename... Types>
class OrcWriterThread {
public:
  OrcWriterThread(std::array<std::string, sizeof...(Types)> column_names, std::string root_folder, std::string file_prefix, uint64_t batchSize, int batchNb_max) {

   this->writer = std::make_unique<Writer<Types...>>(column_names, root_folder, file_prefix, batchSize, batchNb_max);
   writer_thread = std::thread(&OrcWriterThread::consumer, this);
  }

  ~OrcWriterThread() {
    this->stop_thread = true;
    writer_thread.join();
  }

  void write(Types... data) {
    this->fifo.push(std::make_tuple(data...));
  }
  void write(std::tuple<Types...> data) {
    this->fifo.push(data);
  }

  void consumer() {
    while (!this->stop_thread) {
      while (!this->fifo.empty())
      {
        std::tuple<Types...> data = this->fifo.front();
        this->writer->write(data);
        this->fifo.pop();
      }
    }
  }

private:
  std::queue<std::tuple<Types...>> fifo;
  std::thread writer_thread;
  std::unique_ptr<Writer<Types...>> writer;
  bool stop_thread = false; // Super simple thread stopping.
};

} // namespace stryke

#endif // !STRYKE_THREAD_HPP_
