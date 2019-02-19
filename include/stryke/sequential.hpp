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
#ifndef STRYKE_DATE_HPP_
#define STRYKE_DATE_HPP_

#include "date/date.h"
#include "stryke/core.hpp"
#include "stryke/dispatch.hpp"
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

template <typename T, typename... Types>
class OrcWriterSequential : public OrcWriterSequential<T, FolderEncode<>, Types...> {
public:
  OrcWriterSequential(std::array<std::string, sizeof...(Types) + 1> column_names, std::string root_folder, std::string file_prefix, const WriterOptions &options)
      : OrcWriterSequential<T, FolderEncode<>, Types...>(column_names, root_folder, file_prefix, options) {
  }
};

// I don't find a better way. I was not able to implement it with heritage from OrcWriterDispatch<FolderEncode<T, TypesFolder...>, T, Types...>.
template <typename T, typename... TypesFolder, typename... Types>
class OrcWriterSequential<T, FolderEncode<TypesFolder...>, Types...> {
public:
  OrcWriterSequential(std::array<std::string, sizeof...(Types) + sizeof...(TypesFolder) + 1> column_names, std::string root_folder, std::string file_prefix, const WriterOptions &options)
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
    if (data != this->current_data || !this->writer) {
      this->writer = std::make_unique<OrcWriterDispatch<FolderEncode<T, TypesFolder...>, Types...>>(this->column_names_full, this->root_folder, this->file_prefix, this->writeroptions);
      this->current_data = data;
    }
  }

  WriterOptions writeroptions;
  fs::path root_folder;
  std::string file_prefix;

  std::unique_ptr<OrcWriterDispatch<FolderEncode<T, TypesFolder...>, Types...>> writer;
  std::array<std::string, sizeof...(Types) + sizeof...(TypesFolder) + 1> column_names_full;
  T current_data;

};

} // namespace stryke

#endif // !STRYKE_DATE_HPP_
