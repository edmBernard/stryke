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
#include "stryke/core.hpp"
#include "stryke/options.hpp"
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

namespace utils {
template <typename T>
std::chrono::time_point<std::chrono::system_clock> get_time_v1(const T &date);

template <>
inline std::chrono::time_point<std::chrono::system_clock> get_time_v1<Date>(const Date &date) {
  std::istringstream inputStream{date.data};
  date::sys_seconds tp;
  inputStream >> date::parse("%F", tp);
  return tp;
}

template <>
inline std::chrono::time_point<std::chrono::system_clock> get_time_v1<DateNumber>(const DateNumber &date) {
  std::chrono::time_point<std::chrono::system_clock> p1;
  return p1 + std::chrono::seconds(date.data * (60 * 60 * 24));
}

template <>
inline std::chrono::time_point<std::chrono::system_clock> get_time_v1<Timestamp>(const Timestamp &date) {
  std::istringstream inputStream{date.data};
  date::sys_seconds tp;
  inputStream >> date::parse("%F %T", tp);
  return tp;
}

template <>
inline std::chrono::time_point<std::chrono::system_clock> get_time_v1<TimestampNumber>(const TimestampNumber &date) {
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
  }

private:
  bool get_name(T &date) {
    auto tp = utils::get_time_v1(date);
    auto ymd = date::year_month_day(date::floor<date::days>(tp)); // calendar date

    char month_buffer[12]; // for padding
    char day_buffer[12];   // for padding
    sprintf(day_buffer, "%.02d", unsigned(ymd.day()));
    sprintf(month_buffer, "%.02d", unsigned(ymd.month()));

    auto y = int(ymd.year());

    std::string prefix_with_date = this->file_prefix + std::to_string(y) + "-" + std::string(month_buffer) + "-" + std::string(day_buffer);

    // Create new filename if date change or if nbr_batch_max is reached
    if (!this->writers || this->current_prefix_with_date != prefix_with_date || (this->writeroptions.nbr_batch_max > 0 && this->current_counts >= this->writeroptions.batchSize * this->writeroptions.nbr_batch_max)) {

      if (this->current_prefix_with_date != prefix_with_date) {
        this->current_suffix = 0;
      }
      this->current_prefix_with_date = prefix_with_date;

      // Find the right suffix to avoid erasing existing file we check existance
      fs::path file_folder = this->root_folder / ("year=" + std::to_string(y)) / ("month=" + std::string(month_buffer)) / ("day=" + std::string(day_buffer));
      do {
        if (this->writeroptions.suffix_timestamp) {
          this->current_filename = file_folder / (this->current_prefix_with_date + "-" + std::to_string(std::chrono::duration_cast<std::chrono::seconds>(std::chrono::system_clock::now().time_since_epoch()).count()) + ".orc");
        } else {
          this->current_filename = file_folder / (this->current_prefix_with_date + "-" + std::to_string(this->current_suffix++) + ".orc");
        }
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
