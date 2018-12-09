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
#ifndef STRYKE_MULTIFILE_HPP_
#define STRYKE_MULTIFILE_HPP_

#include "stryke_multifile.hpp"
#include <exception>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <map>
#include <string>
#include <vector>

namespace stryke {

namespace fs = std::filesystem;

namespace utils {

// template <typename T>
// bool get_name(T mydate);

// template<>
// bool get_name(Date mydate);

// template <>
// bool get_name(DateNumber mydate) {
//   time_t mytime = mydate.data * (60 * 60 * 24);
//   auto mytm = gmtime(&mytime);
//   std::cout << "years: " << 1900 + mytm->tm_year << std::endl;
//   std::cout << "month: " << 1 + mytm->tm_mon << std::endl;
//   std::cout << "days: " << mytm->tm_mday << std::endl;
//   return true;
// }

// template<>
// bool get_name(Timestamp mydate);

// template<>
// bool get_name(TimestampNumber mydate);

} // namespace utils

//! Writer in mutli file one thread.
//!
//!
template <typename T, typename... Types>
class OrcWriterMulti {
public:
  OrcWriterMulti(std::array<std::string, sizeof...(Types) + 1> column_names, std::string root_folder, std::string file_prefix, uint64_t batchSize, int batchNb_max)
      : column_names(column_names), root_folder(root_folder), file_prefix(file_prefix), batchSize(batchSize) {
  }

  ~OrcWriterMulti() {
  }

  void write(T date, Types... dataT) {
    std::string writers_path = this->get_writer(date);
    this->writers[writers_path]->write(date, dataT...);
  }

private:
  fs::path get_name(T &date) {
    time_t mytime = date.data * (60 * 60 * 24);
    auto mytm = gmtime(&mytime);

    fs::path file_folder = this->root_folder / std::to_string(1900 + mytm->tm_year) / std::to_string(1 + mytm->tm_mon) / std::to_string(mytm->tm_mday);
    std::string prefix_with_date = this->file_prefix + std::to_string(1900 + mytm->tm_year) + "-" + std::to_string(1 + mytm->tm_mon) + "-" + std::to_string(mytm->tm_mday);
    // Add suffix to avoid file erasing
    std::string filename = file_folder / (prefix_with_date + "-0" + ".orc");
    {
      int count = 0;
      while (this->writers.count(filename) == 0 && fs::exists(filename)) {
        filename = file_folder / (prefix_with_date + "-" + std::to_string(count) + ".orc");
        ++count;
      }
    }
    return filename;
  }

  std::string get_writer(T date) {
    fs::path filename = get_name(date);
    if (this->writers.count(filename) == 0) {
      fs::create_directories(filename.parent_path());
      this->writers[filename] = std::make_unique<OrcWriterImpl<T, Types...>>(this->column_names, this->batchSize, this->batchNb_max, filename);
      this->counts[filename] = 0;
    }
    return filename;
  }

  std::array<std::string, sizeof...(Types) + 1> column_names;
  std::map<fs::path, std::unique_ptr<OrcWriterImpl<T, Types...>>> writers;
  std::map<fs::path, int> counts;

  fs::path root_folder;
  std::string file_prefix;
  uint64_t batchSize;
  int batchNb_max;
};

} // namespace stryke

#endif // !STRYKE_MULTIFILE_HPP_