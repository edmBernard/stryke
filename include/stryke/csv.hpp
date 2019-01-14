//
//  Stryke
//
//  https://github.com/edmBernard/Stryke
//
//  Created by Erwan BERNARD on 02/12/2018.
//
//  Copyright (c) 2018 Erwan BERNARD. All rights reserved.
//  Distributed under the Apache License, Version 2.0. (See accompanying
//  file LICENSE or copy at http://www.apache.org/licenses/LICENSE-2.0)
//
#pragma once
#ifndef STRYKE_CSV_HPP_
#define STRYKE_CSV_HPP_

#include "date/date.h"
#include "stryke/options.hpp"

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

namespace utils {
// ==============================================================
//  Create Cast Implementation: function to Cast tuple in json
// ==============================================================

template <typename Types, uint64_t N>
bool addData(std::ofstream &writer, const Types &data, std::string column_name) {
  writer << std::get<N>(data) << ";";
  return true;
}

template <typename... Types, std::size_t... Indices>
auto fillcsvImpl(std::index_sequence<Indices...>, std::ofstream &writer, std::tuple<Types...> &data, std::array<std::string, sizeof...(Types)> column_names) -> std::vector<bool> {
  return {addData<std::tuple<Types...>, Indices>(writer, data, column_names[Indices])...};
}

template <typename... Types>
auto fillcsv(std::ofstream &writer, std::tuple<Types...> &data, std::array<std::string, sizeof...(Types)> column_names) {
  return fillcsvImpl<Types...>(std::index_sequence_for<Types...>(), writer, data, column_names);
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
  CsvWriterImpl(std::array<std::string, sizeof...(Types)> column_names, std::string filename, const WriterOptions &options)
      : writeroptions(options), column_names(column_names), filename(filename) {

    this->writer = std::ofstream(filename);

    if (this->writeroptions.create_lock_file) {
      std::ofstream outfile(this->filename + ".lock");
      outfile.close();
    }
  }

  ~CsvWriterImpl() {
    this->writer.close();
    if (this->writeroptions.create_lock_file) {
      std::filesystem::remove(this->filename + ".lock");
    }
  }

  void write(Types... dataT) {
    this->write_tuple(std::make_tuple(dataT...));
  }

  void write_tuple(std::tuple<Types...> dataT) {
    auto ret = utils::fillcsv(this->writer, dataT, this->column_names);
    this->writer << "\n";
  }


private:

  WriterOptions writeroptions;

  std::ofstream writer;

  std::array<std::string, sizeof...(Types)> column_names;

  std::string filename;
};

} // namespace stryke

#endif // !STRYKE_CSV_HPP_
