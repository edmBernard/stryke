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
#include <condition_variable>
#include <exception>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <map>
#include <mutex>
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
    this->write(std::make_tuple(data...));
  }

  void write(std::tuple<Types...> data) {
    std::unique_lock<std::mutex> lck(this->mx);
    this->fifo.push(data);
    this->queue_is_not_empty.notify_all();
  }

  void consumer() {
    while (!this->stop_thread) {
      std::unique_lock<std::mutex> lck(this->mx, std::defer_lock);
      while (this->fifo.empty()) {
        lck.lock();
        this->queue_is_not_empty.wait(lck);  // Calling wait if lock.mutex() is not locked by the current thread is undefined behavior.
        lck.unlock();
      }

      while (!this->fifo.empty()) {
        lck.lock();
        std::tuple<Types...> data = this->fifo.front();
        this->fifo.pop();
        lck.unlock();

        this->writer->write(data);
      }
    }
  }

private:
  std::queue<std::tuple<Types...>> fifo;
  std::thread writer_thread;
  std::unique_ptr<Writer<Types...>> writer;
  bool stop_thread = false; // Super simple thread stopping.
  std::mutex mx;
  std::condition_variable queue_is_not_empty;
};

} // namespace stryke

#endif // !STRYKE_THREAD_HPP_
