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

#include "date/date.h"
#include "stryke_template.hpp"
#include <exception>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <map>
#include <string>
#include <vector>
#include <ctime>

namespace stryke {

namespace fs = std::filesystem;

namespace utils {
template <typename T>
std::chrono::time_point<std::chrono::system_clock> get_time(const T &date);

template <>
inline std::chrono::time_point<std::chrono::system_clock> get_time<Date>(const Date &date) {
  std::istringstream inputStream{date.data};
  date::sys_seconds tp;
  inputStream >> date::parse("%F", tp);
  return tp;
}

template <>
inline std::chrono::time_point<std::chrono::system_clock> get_time<DateNumber>(const DateNumber &date) {
  std::chrono::time_point<std::chrono::system_clock> p1;
  return p1 + std::chrono::seconds(date.data * (60 * 60 * 24));
}

template <>
inline std::chrono::time_point<std::chrono::system_clock> get_time<Timestamp>(const Timestamp &date) {
  std::istringstream inputStream{date.data};
  date::sys_seconds tp;
  inputStream >> date::parse("%F %T", tp);
  return tp;
}

template <>
inline std::chrono::time_point<std::chrono::system_clock> get_time<TimestampNumber>(const TimestampNumber &date) {
  std::chrono::time_point<std::chrono::system_clock> p1;
  return p1 + std::chrono::seconds(long(date.data));
}

} // namespace utils

//! Writer in mutli file one thread.
//!
//!
template <typename T, typename... Types>
class OrcWriterMulti {
public:
  OrcWriterMulti(std::array<std::string, sizeof...(Types) + 1> column_names, std::string root_folder, std::string file_prefix, const WriterOptions &options)
      : writeroptions(options), column_names(column_names), root_folder(root_folder), file_prefix(file_prefix) {
  }

  ~OrcWriterMulti() {
  }

  void write(T date, Types... dataT) {
    this->write_tuple(std::make_tuple(date, dataT...));
  }

  void write_tuple(std::tuple<T, Types...> dataT) {
    this->get_writer(std::get<0>(dataT));
    this->writers->write_tuple(dataT);
    ++this->current_counts;
  }

  void close() {
    this->writers.reset();
    this->current_prefix_with_date = std::string();
    this->current_counts = 0;
  }

private:
  bool get_name(T &date) {
    auto tp = utils::get_time(date);
    auto ymd = date::year_month_day(date::floor<date::days>(tp));   // calendar date

    auto y   = int(ymd.year());
    auto m   = unsigned(ymd.month());
    auto d   = unsigned(ymd.day());

    fs::path file_folder = this->root_folder / ("year=" + std::to_string(y)) / ("month=" + std::to_string(m)) / ("day=" + std::to_string(d));
    std::string prefix_with_date = this->file_prefix + std::to_string(y) + "-" + std::to_string(m) + "-" + std::to_string(d);

    // Create new filename if date change or if nbr_batch_max is reached
    if (this->current_prefix_with_date.empty() || this->current_prefix_with_date != prefix_with_date || (this->writeroptions.nbr_batch_max > 0 && this->current_counts >= this->writeroptions.batchSize * this->writeroptions.nbr_batch_max)) {
      this->current_suffix = 0;
      this->current_prefix_with_date = prefix_with_date;

      // Find the right suffix to avoid erasing existing file we check existance
      do {
        this->current_filename = file_folder / (this->current_prefix_with_date + "-" + std::to_string(this->current_suffix++) + ".orc");
      } while (fs::exists(this->current_filename));

      return true;
    }
    return false;
  }

  void get_writer(T date) {
    if (get_name(date)) {
      fs::create_directories(this->current_filename.parent_path());
      this->writers = std::make_unique<OrcWriterImpl<T, Types...>>(this->column_names, this->current_filename, this->writeroptions);
      this->current_counts = 0;
    }
  }

  WriterOptions writeroptions;
  std::array<std::string, sizeof...(Types) + 1> column_names;

  std::string current_prefix_with_date;
  fs::path current_filename;
  std::unique_ptr<OrcWriterImpl<T, Types...>> writers;
  int current_suffix;
  uint64_t current_counts;

  fs::path root_folder;
  std::string file_prefix;
};

} // namespace stryke

#endif // !STRYKE_MULTIFILE_HPP_
