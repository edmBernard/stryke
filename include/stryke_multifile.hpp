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

#include "stryke_template.hpp"
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
class OrcWriterMulti {
public:
  OrcWriterMulti(std::array<std::string, sizeof...(Types) + 1> column_names, std::string root_folder, std::string file_prefix, uint64_t batchSize = 10000, int nbr_batch_max = 0)
      : column_names(column_names), root_folder(root_folder), file_prefix(file_prefix), batchSize(batchSize), nbr_batch_max(nbr_batch_max) {
  }

  ~OrcWriterMulti() {
  }

  void write(T date, Types... dataT) {
    this->write(std::make_tuple(date, dataT...));
  }

  void write(std::tuple<T, Types...> dataT) {
    this->get_writer(std::get<0>(dataT));
    this->writers->write(dataT);
    ++this->current_counts;
  }

  void close() {
    this->writers.reset();
    this->current_prefix_with_date = std::string();
    this->current_counts = 0;
  }

private:
  bool get_name(T &date) {
    time_t mytime = date.data * (60 * 60 * 24);
    auto mytm = gmtime(&mytime);

    fs::path file_folder = this->root_folder / std::to_string(1900 + mytm->tm_year) / std::to_string(1 + mytm->tm_mon) / std::to_string(mytm->tm_mday);
    std::string prefix_with_date = this->file_prefix + std::to_string(1900 + mytm->tm_year) + "-" + std::to_string(1 + mytm->tm_mon) + "-" + std::to_string(mytm->tm_mday);


    // Create new filename if date change or if nbr_batch_max is reached
    if (this->current_prefix_with_date.empty() || this->current_prefix_with_date != prefix_with_date || (this->nbr_batch_max > 0 && this->current_counts >= this->batchSize * this->nbr_batch_max)) {
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
      this->writers = std::make_unique<OrcWriterImpl<T, Types...>>(this->column_names, this->current_filename, this->batchSize);
      this->current_counts = 0;
    }
  }

  std::array<std::string, sizeof...(Types) + 1> column_names;

  std::string current_prefix_with_date;
  fs::path current_filename;
  std::unique_ptr<OrcWriterImpl<T, Types...>> writers;
  int current_suffix;
  uint64_t current_counts;

  fs::path root_folder;
  std::string file_prefix;
  uint64_t batchSize;
  int nbr_batch_max;
};

} // namespace stryke

#endif // !STRYKE_MULTIFILE_HPP_
