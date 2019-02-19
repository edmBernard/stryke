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
class OrcWriterDate : public OrcWriterDate<T, FolderEncode<>, Types...> {
public:
  OrcWriterDate(std::array<std::string, sizeof...(Types) + 1> column_names, std::string root_folder, std::string file_prefix, const WriterOptions &options)
      : OrcWriterDate<T, FolderEncode<>, Types...>(column_names, root_folder, file_prefix, options) {
  }
};

template <typename T, typename... TypesFolder, typename... Types>
class OrcWriterDate<T, FolderEncode<TypesFolder...>, Types...> {

  std::unique_ptr<OrcWriterDispatch<FolderEncode<T, TypesFolder...>, T, Types...>> writer;
  std::array<std::string, sizeof...(Types) + sizeof...(TypesFolder) + 2> column_names_full;

public:
  OrcWriterDate(std::array<std::string, sizeof...(Types) + sizeof...(TypesFolder) + 1> column_names, std::string root_folder, std::string file_prefix, const WriterOptions &options) {
    std::copy_n(column_names.begin(), sizeof...(TypesFolder) + 1, this->column_names_full.begin());
    std::copy_n(column_names.begin() + sizeof...(TypesFolder) + 1, sizeof...(Types), this->column_names_full.begin() + sizeof...(Types) + 1);
    std::copy_n(column_names.begin(), 1, this->column_names_full.begin() + sizeof...(Types));
    for (auto i : this->column_names_full) {
      std::cout << "i : " << i << std::endl;
    }
    this->writer = std::make_unique<OrcWriterDispatch<FolderEncode<T, TypesFolder...>, T, Types...>>(this->column_names_full, root_folder, file_prefix, options);
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

} // namespace stryke

#endif // !STRYKE_DATE_HPP_
