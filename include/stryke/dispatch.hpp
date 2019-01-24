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

#include "date/date.h"
#include "stryke/core.hpp"
#include "stryke/options.hpp"
#include "stryke/multifile.hpp"
#include <ctime>
#include <exception>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <map>
#include <string>
#include <vector>

namespace stryke {

namespace fs = std::filesystem;

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

  void close() {
    this->writers.clear();
    this->counts.clear();
  }

private:
   std::string get_writer(T date) {
    // Process Date
    auto tp = utils::get_time(date);
    auto ymd = date::year_month_day(date::floor<date::days>(tp)); // calendar date
    char month_buffer[12]; // for padding
    char day_buffer[12];   // for padding
    sprintf(day_buffer, "%.02d", unsigned(ymd.day()));
    sprintf(month_buffer, "%.02d", unsigned(ymd.month()));
    auto y = int(ymd.year());

    // Get prefix_with_date
    std::string prefix_with_date = this->file_prefix + std::to_string(y) + "-" + std::string(month_buffer) + "-" + std::string(day_buffer);

    // Get folder with date
    fs::path file_folder = this->root_folder / ("year=" + std::to_string(y)) / ("month=" + std::string(month_buffer)) / ("day=" + std::string(day_buffer));

    if (this->writers.count(prefix_with_date) == 0) {
      this->counts[prefix_with_date] = -1;

      fs::path filename;
      do {
        filename = file_folder / (prefix_with_date + "-" + std::to_string(++this->counts[prefix_with_date]) + ".orc");
      } while (fs::exists(filename));

      fs::create_directories(filename.parent_path());
      this->writers[prefix_with_date] = std::make_unique<OrcWriterImpl<T, Types...>>(this->column_names, filename, this->writeroptions);
    }
    return prefix_with_date;
  }

  WriterOptions writeroptions;
  std::array<std::string, sizeof...(Types) + 1> column_names;
  std::map<fs::path, std::unique_ptr<OrcWriterImpl<T, Types...>>> writers;
  std::map<fs::path, int> counts;  // counts for suffix in filename

  fs::path root_folder;
  std::string file_prefix;
};

} // namespace stryke

#endif // !STRYKE_MULTIFILE_HPP_
