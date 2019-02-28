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
#include <fstream>
#include <iostream>
#include <sstream>
#include <string>


int main(int argc, char const *argv[]) {
  if (argc != 2) {
    std::cout << "Usage: stryke_writer_rainbow <filename>\n"
              << "Write a file with 11 column with different type : <filename>.\n"
              << "The writen file will have the follwing column :\n"
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

    stryke::WriterOptions options;

    stryke::OrcWriterImpl<
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
      stryke::TimestampNumber> writer(
        {"Long",
         "Short",
         "Int",
         "String",
         "Double",
         "Float",
         "Boolean",
         "Date",
         "DateNumber",
         "Timestamp",
         "TimestampNumber"}, argv[1], options);

    for (int i = 0; i < 10; ++i) {
      writer.write(i*10, i*100, i*1000, std::to_string(i*10000), i/10., i/100., i%3==0, "2018-12-18", i, "2018-12-18 12:26:20.123456789", i);
    }

  } catch (std::exception &ex) {
    std::cerr << "Caught exception: " << ex.what() << "\n";
    return 1;
  }
  return 0;
}
