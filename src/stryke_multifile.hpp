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

#include "stryke_multifile.hpp"
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
template <typename... Types>
class OrcWriterMulti {
public:
  OrcWriterMulti(std::array<std::string, sizeof...(Types)> column_names, std::string root_folder, std::string prefix, uint64_t batchSize, int batchNb_max)
      : column_names(column_names), batchSize(batchSize) {
  }
  void write(Types... dataT) {
    for (auto &&i : this->writers) {
      i.second.write(dataT...);
    }
  }

  std::array<std::string, sizeof...(Types)> column_names;
  std::map<fs::path, OrcWriterImpl<Types...>> writers;

  fs::path root_folder;
  std::string prefix;
  uint64_t batchSize;
  int batchNb_max;
};

} // namespace stryke

#endif // !STRYKE_MULTIFILE_HPP_