//
//  Stryke
//
//  https://github.com/edmBernard/Stryke
//
//  Created by Erwan BERNARD on 22/12/2018.
//
//  Copyright (c) 2018 Erwan BERNARD. All rights reserved.
//  Distributed under the Apache License, Version 2.0. (See accompanying
//  file LICENSE or copy at http://www.apache.org/licenses/LICENSE-2.0)
//

#include "date/date.h"
#include "stryke/core.hpp"
#include "stryke/reader.hpp"
#include <filesystem>
#include <fstream>
#include <iostream>
#include <sstream>
#include <regex>
#include <string>

namespace fs = std::filesystem;

int main(int argc, char const *argv[]) {
  if (argc != 3) {
    std::cout << "Usage: stryke_merge_file <sourcefolder> <outputfilename>\n"
              << "Merge file from a folder <sourcefolder> into one file <outputfilename>.\n"
              << "Output column name in the outputfile will be : 'timestamp','col1','col2','col3','col4'\n";
    return 1;
  }

  try {

    uint64_t count = 0;
    auto writer = stryke::OrcWriterImpl<stryke::TimestampNumber, stryke::Long, stryke::Long, stryke::Long, stryke::Long>({"timestamp","col1","col2","col3","col4"}, argv[2], stryke::WriterOptions());
    for (auto &p : fs::directory_iterator(argv[1])) {
      if (p.is_regular_file()) {
        if (std::regex_match(p.path().string(), std::regex(".*\\.orc"))) {
          auto reader = stryke::OrcReader<stryke::TimestampNumber, stryke::Long, stryke::Long, stryke::Long, stryke::Long>(p.path().string());
          auto result = reader.get_data();
          for (auto &&i : result) {
            ++count;
            writer.write_tuple(i);
          }
        }
      }
    }

    std::cout << count << " lines merged in : " << argv[2] << std::endl;

  } catch (std::exception &ex) {
    std::cerr << "Caught exception: " << ex.what() << "\n";
    return 1;
  }
  return 0;
}
