//
//  Stryke
//
//  https://github.com/edmBernard/Stryke
//
//  Created by Erwan BERNARD on 08/12/2018.
//
//  Copyright (c) 2018, 2019, 2020, 2021 Erwan BERNARD. All rights reserved.
//  Distributed under the Apache License, Version 2.0. (See accompanying
//  file LICENSE or copy at http://www.apache.org/licenses/LICENSE-2.0)
//
#pragma once

#include "date/date.h"
#include "stryke/options.hpp"
#include "stryke/type.hpp"
#include <atomic>
#include <condition_variable>
#include <exception>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <map>
#include <mutex>
#include <queue>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

namespace stryke {

namespace fs = std::filesystem;

namespace utils {

} // namespace utils

//! Wrapper arroud a writer to delegate file writing in a separated thread.
//!
//! \tparam Writer Writer to use. OrcWriterSequentialDuplicate, OrcWriterDispatchDuplicate
//! \tparam T Data that will be duplicate between folder and file. It behaviour change in function of used writer
//! \tparam Types Types list of field stryke::Type or stryke::FolderEncode.
//!
template <template <typename...> typename Writer, typename T, typename... Types>
class OrcWriterThread : public OrcWriterThread<Writer, T, FolderEncode<>, Types...> {
public:
  OrcWriterThread(std::array<std::string, sizeof...(Types) + 1> column_names, std::filesystem::path root_folder, std::string file_prefix, const WriterOptions &options)
      : OrcWriterThread<Writer, T, FolderEncode<>, Types...>(column_names, root_folder, file_prefix, options) {
  }
};

//! Wrapper arroud a writer to delegate file writing in a separated thread.
//!
//! \tparam Writer Writer to use. OrcWriterSequentialDuplicate, OrcWriterDispatchDuplicate
//! \tparam T Data that will be duplicate between folder and file. It behaviour change in function of used writer
//! \tparam TypesFolder Types list of field stryke::Type contained in a stryke::FolderEncode. These datas will be store in folder path.
//! \tparam Types Types list of field stryke::Type.
//!
template <template <typename...> typename Writer, typename T, typename... TypesFolder, typename... Types>
class OrcWriterThread<Writer, T, FolderEncode<TypesFolder...>, Types...> {
public:
  OrcWriterThread(std::array<std::string, 1 + sizeof...(TypesFolder) + sizeof...(Types)> column_names, std::filesystem::path root_folder, std::string file_prefix, const WriterOptions &options) {
    this->writer = std::make_unique<Writer<T, FolderEncode<TypesFolder...>, Types...>>(column_names, root_folder, file_prefix, options);
    this->cron_minute = options.cron;
    this->writer_thread = std::thread(&OrcWriterThread::consumer, this);
    if (this->cron_minute > 0) {
      this->cron_thread = std::thread(&OrcWriterThread::cron, this);
    }
  }

  //! Destroy the Orc Writer Thread object. Close file, stop thread etc ...
  //!
  //!
  ~OrcWriterThread() {
    // close_sync
    this->writer_closed = false;                      // unecessary but iso with async close
    this->close_writer = true;                        // ask writer thread to close writer
    this->queue_is_not_empty.notify_all();            // command to unlock writer thread if queue is already empty
    std::unique_lock<std::mutex> lck(this->mx_close); // lock only to block close method until file is closed
    while (!this->fifo.empty()) {
      this->writer_is_closed.wait_for(lck, std::chrono::duration<double, std::milli>(100)); // calling wait if lock.mutex() is not locked by the current thread is undefined behavior.
    }
    // end close_sync

    // Stop thread
    this->stop_thread = true;
    this->writer_thread.join();
    if (this->cron_minute > 0) {
      this->cron_thread.join();
    }
  }

  //! Write given data to file.
  //!
  //! \param data Data stored in folder and in file.
  //! \param datafolder Data encoded in folder
  //! \param dataT Data stored in file
  //!
  void write(T date, TypesFolder... datafolder, Types... dataT) {
    this->write_tuple(std::make_tuple(date, datafolder...), std::make_tuple(date, dataT...));
  }

  //! Write given tuple to file.
  //!
  //! \param datafolder Data encoded in folder. The first element is the trigger for file closing.
  //! \param dataT Data stored in file
  //!
  void write_tuple(std::tuple<T, TypesFolder...> datafolder, std::tuple<T, Types...> dataT) {
    std::unique_lock<std::mutex> lck(this->mx_queue);  // lock to protect read and write on queue
    std::unique_lock<std::mutex> lck2(this->mx_close); // lock only to block close method until file is closed
    this->fifo.push(dataT);
    this->fifo_folder.push(datafolder);
    this->queue_is_not_empty.notify_all();
  }

  //! Close file and finalize it.
  //!
  //!
  bool has_closed() {
    return this->writer_closed;
  }

  //! Close Writer Asynchronously.
  //! I made the choice that close_async wait the queue to be empty before closing file.
  //! if we continue to write data fast enough the file can never close.
  //!
  //!
  void close_async() {
    this->writer_closed = false;
    this->close_writer = true;             // ask writer thread to close writer
    this->queue_is_not_empty.notify_all(); // command to unlock writer thread if queue is already empty
  }

  //! Close Writer Synchronously.
  //!
  //!
  void close_sync() {
    this->writer_closed = false;                      // unecessary but iso with async close
    this->close_writer = true;                        // ask writer thread to close writer
    this->queue_is_not_empty.notify_all();            // command to unlock writer thread if queue is already empty
    std::unique_lock<std::mutex> lck(this->mx_close); // lock only to block close method until file is closed
    while (!this->fifo.empty()) {
      this->writer_is_closed.wait_for(lck, std::chrono::duration<double, std::milli>(100)); // calling wait if lock.mutex() is not locked by the current thread is undefined behavior.
    }
  }

private:
  //! Function that consume data from fifo and write them in file.
  //!
  //!
  void consumer() {
    while (!this->stop_thread) {
      std::unique_lock<std::mutex> lck(this->mx_queue, std::defer_lock); // lock to protect read and write on queue
      if (this->fifo.empty()) {
        if (this->close_writer) {
          this->writer->close();
          this->close_writer = false;
          this->writer_closed = true;          // variable for check after async close
          this->writer_is_closed.notify_all(); // signal to unlock sync close
        }
        lck.lock();
        this->queue_is_not_empty.wait_for(lck, std::chrono::duration<double, std::milli>(100)); // calling wait if lock.mutex() is not locked by the current thread is undefined behavior.
        lck.unlock();
      }

      while (!this->fifo.empty()) {
        lck.lock();
        std::tuple<T, Types...> data = this->fifo.front();
        std::tuple<T, TypesFolder...> data_folder = this->fifo_folder.front();
        this->fifo.pop();
        this->fifo_folder.pop();
        lck.unlock();
        this->writer->write_tuple(data_folder, data);
      }
    }
  }

  //! Function that trigger file closing periodically.
  //!
  //!
  void cron() {
    long previous_minute_count = 0;

    while (!this->stop_thread) {

      auto tp = std::chrono::system_clock::now();
      auto daypoint = date::floor<date::days>(tp);
      auto tod = date::make_time(tp - daypoint);

      auto minute_count = tod.hours().count() * 60 + tod.minutes().count();
      if ((minute_count) % this->cron_minute == 0) {
        if (previous_minute_count != minute_count) {
          this->close_sync();
          previous_minute_count = minute_count;
        }
      }
    }
  }

  std::queue<std::tuple<T, Types...>> fifo;                                  //!< First in First out file use to store data store in file between main thread and writer thread
  std::queue<std::tuple<T, TypesFolder...>> fifo_folder;                     //!< First in First out file use to store data store in folder between main thread and writer thread
  std::thread writer_thread;                                                 //!< Thread with writer function
  std::thread cron_thread;                                                   //!< Thread with cron function
  int cron_minute;                                                           //!< Trigger for cron
  std::unique_ptr<Writer<T, FolderEncode<TypesFolder...>, Types...>> writer; //!< Writer use to write Orc file
  std::atomic<bool> stop_thread = false;                                     //!< simple thread stopping.
  std::atomic<bool> close_writer = false;                                    //!< ask writer thread to close writer
  std::atomic<bool> writer_closed = true;                                    //!< variable for check after async close
  std::mutex mx_queue;                                                       //!< lock to protect read and write on queue
  std::mutex mx_close;                                                       //!< lock only to block close method until file is closed
  std::condition_variable queue_is_not_empty;                                //!< Variable to trigger signal between thread
  std::condition_variable writer_is_closed;                                  //!< Variable to trigger signal between thread
};

} // namespace stryke
