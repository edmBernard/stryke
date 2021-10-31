//
//  Stryke
//
//  https://github.com/edmBernard/Stryke
//
//  Created by Erwan BERNARD on 10/12/2018.
//
//  Copyright (c) 2018 Erwan BERNARD. All rights reserved.
//  Distributed under the Apache License, Version 2.0. (See accompanying
//  file LICENSE or copy at http://www.apache.org/licenses/LICENSE-2.0)
//
#pragma once

#include "orc/OrcFile.hh"
#include "orc/Reader.hh"
#include "stryke/core.hpp"

#include <string>
#include <filesystem>

namespace stryke {

struct BasicStats {
  uint64_t nbr_rows;
  uint32_t nbr_columns;
};

struct ColumnsStats {
  uint64_t nbr_value;
  std::string type;
  uint64_t min;
  uint64_t max;
  uint64_t sum;
};

inline BasicStats get_basic_stats(std::filesystem::path filename) {
  BasicStats file_stats;
  std::unique_ptr<orc::Reader> reader = orc::createReader(orc::readFile(filename.string()), orc::ReaderOptions());
  std::unique_ptr<orc::Statistics> colStats = reader->getStatistics();

  file_stats.nbr_columns = colStats->getNumberOfColumns();
  file_stats.nbr_rows = reader->getNumberOfRows();

  return file_stats;
};

} // namespace Stryke
