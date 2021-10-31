//
//  Stryke
//
//  https://github.com/edmBernard/Stryke
//
//  Created by Erwan BERNARD on 02/12/2018.
//
//  Copyright (c) 2018, 2019 Erwan BERNARD. All rights reserved.
//  Distributed under the Apache License, Version 2.0. (See accompanying
//  file LICENSE or copy at http://www.apache.org/licenses/LICENSE-2.0)
//
#pragma once
#ifndef STRYKE_CSV_HPP_
#define STRYKE_CSV_HPP_

#include "date/date.h"
#include "stryke/options.hpp"
#include "stryke/type.hpp"

#include <array>
#include <cmath>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <memory>
#include <queue>
#include <regex>
#include <sstream>
#include <string>
#include <thread>
#include <tuple>

namespace stryke {
namespace csv {

namespace fs = std::filesystem;

namespace utils {

// ==============================================================
//  Fill file with data
// ==============================================================

template <typename Types, uint64_t N>
bool addData(std::ofstream &writer, const Types &data) {
  writer << std::setprecision(20) << std::get<N>(data) << ";";
  return true;
}

template <typename... Types, std::size_t... Indices>
auto fillcsvImpl(std::index_sequence<Indices...>, std::ofstream &writer, std::tuple<Types...> &data) -> std::vector<bool> {
  return {addData<std::tuple<Types...>, Indices>(writer, data)...};
}

template <typename... Types>
auto fillcsv(std::ofstream &writer, std::tuple<Types...> &data) {
  return fillcsvImpl<Types...>(std::index_sequence_for<Types...>(), writer, data);
}

} // namespace utils


// ==============================================================
//  Simple Writer Implementation
// ==============================================================

//! Writer in one file one thread.
//!
//!
template <typename... Types>
class CsvWriterImpl {
public:
  CsvWriterImpl(std::array<std::string, sizeof...(Types)> column_names, std::filesystem::path root_folder, std::string filename, const WriterOptions &options)
      : writeroptions(options), column_names(column_names), root_folder(root_folder), filename(filename) {

    this->writer = std::ofstream(filename);

    // Add column name
    for (auto& i : this->column_names) {
      this->writer << i << ";";
    }
    this->writer << "\n";

    // create lock file
    if (this->writeroptions.create_lock_file) {
      std::ofstream outfile(this->root_folder / (this->filename + ".lock"));
      outfile.close();
    }
  }

  CsvWriterImpl(std::array<std::string, sizeof...(Types)> column_names, std::string filename, const WriterOptions &options)
      : CsvWriterImpl(column_names, "", filename, options) {
  }

  ~CsvWriterImpl() {
    this->writer.close();
    if (this->writeroptions.create_lock_file) {
      std::filesystem::remove(this->root_folder / (this->filename + ".lock"));
    }
  }

  void write(Types... dataT) {
    this->write_tuple(std::make_tuple(dataT...));
  }

  void write_tuple(std::tuple<Types...> dataT) {
    auto ret = utils::fillcsv(this->writer, dataT);
    this->writer << "\n";
  }

  uint64_t const &get_count() const {
    return this->total_line;
  }

private:

  WriterOptions writeroptions;

  std::ofstream writer;

  std::array<std::string, sizeof...(Types)> column_names;

  fs::path root_folder;
  std::string filename;

  uint64_t numValues = 0; // num of lines in batch writer
  uint64_t total_line = 0; // num of lines in batch writer + number of line already write in file
};

} // namespace csv
} // namespace stryke

#endif // !STRYKE_CSV_HPP_
