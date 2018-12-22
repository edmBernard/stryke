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
#ifndef STRYKE_DISPATCH_HPP_
#define STRYKE_DISPATCH_HPP_

#include "stryke/core.hpp"
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


} // namespace utils

//! Writer in mutli file one thread.
//!
//!
template <typename T, typename... Types>
class OrcWriterDispatch {
public:
  OrcWriterDispatch(std::array<std::string, sizeof...(Types) + 1> column_names, std::string root_folder, std::string file_prefix, const WriterOptions &options)
      : writeroptions(options), column_names(column_names), root_folder(root_folder), file_prefix(file_prefix) {
  }

  ~OrcWriterDispatch() {
  }

  void write(T date, Types... dataT) {
    this->write_tuple(std::make_tuple(date, dataT...));
  }

  void write_tuple(std::tuple<T, Types...> dataT) {
    std::string writers_path = this->get_writer(std::get<0>(dataT));
    this->writers[writers_path]->write_tuple(dataT);
  }

private:
  fs::path get_name(T &date) {
    time_t mytime = date.data * (60 * 60 * 24);
    auto mytm = gmtime(&mytime);

    char month_buffer[12];  // for padding
    char day_buffer[12];  // for padding
    sprintf(day_buffer, "%.02d", mytm->tm_mday);
    sprintf(month_buffer, "%.02d", 1 + mytm->tm_mon);
    fs::path file_folder = this->root_folder / ("year="+std::to_string(1900 + mytm->tm_year)) / ("month="+std::string(month_buffer)) / ("day="+std::string(day_buffer));
    std::string prefix_with_date = this->file_prefix + std::to_string(1900 + mytm->tm_year) + "-" + std::string(month_buffer) + "-" + std::string(day_buffer);

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
      this->writers[filename] = std::make_unique<OrcWriterImpl<T, Types...>>(this->column_names, filename, this->writeroptions);
      this->counts[filename] = 0;
    }
    return filename;
  }

  WriterOptions writeroptions;

  std::array<std::string, sizeof...(Types) + 1> column_names;
  std::map<fs::path, std::unique_ptr<OrcWriterImpl<T, Types...>>> writers;
  std::map<fs::path, int> counts;

  fs::path root_folder;
  std::string file_prefix;
};

} // namespace stryke

#endif // !STRYKE_DISPATCH_HPP_
