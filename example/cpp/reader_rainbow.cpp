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
#include <fstream>
#include <iostream>
#include <sstream>
#include <string>


int main(int argc, char const *argv[]) {
  if (argc != 2) {
    std::cout << "Usage: stryke_reader_rainbow <filename>\n"
              << "Print contents of the single columns orc file : <filename>.\n"
              << "The writen file would have the follwing column :\n"
              << "      -  Column 1 for Long\n"
              << "      -  Column 2 for Short\n"
              << "      -  Column 3 for Int\n"
              << "      -  Column 4 for String\n"
              << "      -  Column 5 for Double\n"
              << "      -  Column 6 for Float\n"
              << "      -  Column 7 for Boolean\n"
              << "      -  Column 8 for Date\n"
              << "      -  Column 9 for DateNumber\n"
              << "      -  Column 10 for Timestamp\n"
              << "      -  Column 11 for TimestampNumber\n";
    return 1;
  }

  try {

    auto reader = stryke::OrcReader<
      stryke::Long,
      stryke::Short,
      stryke::Int,
      stryke::String,
      stryke::Double,
      stryke::Float,
      stryke::Boolean,
      stryke::Date,
      stryke::DateNumber,
      stryke::Timestamp,
      stryke::TimestampNumber>(argv[1]);


    for (size_t i = 0; i < reader.get_cols_name().size(); ++i) {
      std::cout << "column " << i << " : " << reader.get_cols_name()[i] << std::endl;
    }

    int count = 0;

    // get all data in one time
    for (auto&& i : reader.get_data()) {
      std::cout << "**** Row: " << count++ << std::endl
                << "  - Long: " << std::get<0>(i).data << std::endl
                << "  - Short: " << std::get<1>(i).data << std::endl
                << "  - Int: " << std::get<2>(i).data << std::endl
                << "  - String: " << std::get<3>(i).data << std::endl
                << "  - Double: " << std::get<4>(i).data << std::endl
                << "  - Float: " << std::get<5>(i).data << std::endl
                << "  - Boolean: " << std::get<6>(i).data << std::endl
                << "  - Date: " << std::get<7>(i).data << std::endl
                << "  - DateNumber: " << std::get<8>(i).data << std::endl
                << "  - Timestamp: " << std::get<9>(i).data << std::endl
                << "  - TimestampNumber: " << std::get<10>(i).data << std::endl;
    }

    // reset reader to be able to read data another time
    reader.reset();

    // read data by batch
    for (auto res = reader.get_batch(); !res.empty(); res = reader.get_batch()) {
      std::cout << "res.size() : " << res.size() << std::endl;
    }

  } catch (std::exception &ex) {
    std::cerr << "Caught exception: " << ex.what() << "\n";
    return 1;
  }
  return 0;
}
