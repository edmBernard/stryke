//
//  Stryke
//
//  https://github.com/edmBernard/Stryke
//
//  Created by Erwan BERNARD on 03/04/2019.
//
//  Copyright (c) 2018, 2019 Erwan BERNARD. All rights reserved.
//  Distributed under the Apache License, Version 2.0. (See accompanying
//  file LICENSE or copy at http://www.apache.org/licenses/LICENSE-2.0)
//
#pragma once
#ifndef STRYKE_CSV_SEQUENTIAL_HPP_
#define STRYKE_CSV_SEQUENTIAL_HPP_

#include "date/date.h"
#include "stryke/csv/dispatch.hpp"
#include "stryke/options.hpp"
#include "stryke/type.hpp"
#include <ctime>
#include <exception>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <map>
#include <sstream>
#include <string>
#include <vector>

namespace stryke {
namespace csv {

namespace utils {

template <typename T>
bool compare(T value1, T value2) {
  return value1.data == value2.data;
}

template <>
inline bool compare(TimestampNumber value1, TimestampNumber value2) {
  return long(value1.data/86400) == long(value2.data/86400);
}

template <>
inline bool compare(Double value1, Double value2) {
  return long(value1.data) == long(value2.data);
}

} // namespace utils

template <typename T, typename... Types>
class CsvWriterSequential : public CsvWriterSequential<T, FolderEncode<>, Types...> {
public:
  CsvWriterSequential(std::array<std::string, sizeof...(Types) + 1> column_names, std::string root_folder, std::string file_prefix, const WriterOptions &options)
      : CsvWriterSequential<T, FolderEncode<>, Types...>(column_names, root_folder, file_prefix, options) {
  }
};

// I don't find a better way. I was not able to implement it with heritage from CsvWriterDispatch<FolderEncode<T, TypesFolder...>, T, Types...>.
template <typename T, typename... TypesFolder, typename... Types>
class CsvWriterSequential<T, FolderEncode<TypesFolder...>, Types...> {
public:
  CsvWriterSequential(std::array<std::string, sizeof...(Types) + sizeof...(TypesFolder) + 1> column_names, std::string root_folder, std::string file_prefix, const WriterOptions &options)
      : writeroptions(options), root_folder(root_folder), file_prefix(file_prefix) {
    std::copy_n(column_names.begin(), sizeof...(TypesFolder) + 1, this->column_names_full.begin());
    std::copy_n(column_names.begin() + sizeof...(TypesFolder) + 1, sizeof...(Types), this->column_names_full.begin() + sizeof...(TypesFolder) + 1);
  }

  void write(T data, TypesFolder... datafolder, Types... dataT) {
    this->write_tuple(std::make_tuple(data, datafolder...), std::make_tuple(dataT...));
  }

  void write_tuple(std::tuple<T, TypesFolder...> datafolder, std::tuple<Types...> dataT) {
    this->get_writer(std::get<0>(datafolder));
    this->writer->write_tuple(datafolder, dataT);
  }

  void close() {
    this->writer->close();
  }

private:
  void get_writer(T data) {
    if (!utils::compare(data, this->current_data) || !this->writer) {
      this->writer = std::make_unique<CsvWriterDispatch<FolderEncode<T, TypesFolder...>, Types...>>(this->column_names_full, this->root_folder, this->file_prefix, this->writeroptions);
      this->current_data = data;
    }
  }

  WriterOptions writeroptions;
  fs::path root_folder;
  std::string file_prefix;

  std::unique_ptr<CsvWriterDispatch<FolderEncode<T, TypesFolder...>, Types...>> writer;
  std::array<std::string, sizeof...(Types) + sizeof...(TypesFolder) + 1> column_names_full;
  T current_data;
};

template <typename T, typename... Types>
class CsvWriterSequentialDuplicate : public CsvWriterSequentialDuplicate<T, FolderEncode<>, Types...> {
public:
  CsvWriterSequentialDuplicate(std::array<std::string, sizeof...(Types) + 1> column_names, std::string root_folder, std::string file_prefix, const WriterOptions &options)
      : CsvWriterSequentialDuplicate<T, FolderEncode<>, Types...>(column_names, root_folder, file_prefix, options) {
  }
};

// I don't find a better way. I was not able to implement it with heritage from CsvWriterSequential<FolderEncode<T, TypesFolder...>, T, Types...>.
template <typename T, typename... TypesFolder, typename... Types>
class CsvWriterSequentialDuplicate<T, FolderEncode<TypesFolder...>, Types...> {

  std::unique_ptr<CsvWriterSequential<T, FolderEncode<TypesFolder...>, T, Types...>> writer;
  std::array<std::string, sizeof...(Types) + sizeof...(TypesFolder) + 2> column_names_full;

public:
  CsvWriterSequentialDuplicate(std::array<std::string, sizeof...(Types) + sizeof...(TypesFolder) + 1> column_names, std::string root_folder, std::string file_prefix, const WriterOptions &options) {
    std::copy_n(column_names.begin(), sizeof...(TypesFolder) + 1, this->column_names_full.begin());
    std::copy_n(column_names.begin() + sizeof...(TypesFolder) + 1, sizeof...(Types), this->column_names_full.begin() + sizeof...(TypesFolder) + 2);
    std::copy_n(column_names.begin(), 1, this->column_names_full.begin() + sizeof...(TypesFolder) + 1);

    this->writer = std::make_unique<CsvWriterSequential<T, FolderEncode<TypesFolder...>, T, Types...>>(this->column_names_full, root_folder, file_prefix, options);
  }

  void write(T date, TypesFolder... datafolder, Types... dataT) {
    this->writer->write(date, datafolder..., date, dataT...);
  }

  void write_tuple(std::tuple<T, TypesFolder...> datafolder, std::tuple<T, Types...> dataT) {
    this->writer->write_tuple(datafolder, dataT);
  }

  void close() {
    this->writer->close();
  }
};

} // namespace csv
} // namespace stryke

#endif // !STRYKE_CSV_SEQUENTIAL_HPP_
