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

enum StrykeType {
  Long = 1,
  Short = 2,
  Int = 3,
  String = 4,
  Double = 5,
  Float = 6,
  Boolean = 7,
  Date = 8,
  DateNumber = 9,
  Timestamp = 10,
  TimestampNumber = 11
};

int main(int argc, char const *argv[]) {
  if (argc != 3) {
    std::cout << "Usage: stryke_reader_basic_type <type> <filename>\n"
              << "Print contents of the single columns orc file : <filename>.\n"
              << "<type> can be :\n"
              << "      -  1 for Long\n"
              << "      -  2 for Short\n"
              << "      -  3 for Int\n"
              << "      -  4 for String\n"
              << "      -  5 for Double\n"
              << "      -  6 for Float\n"
              << "      -  7 for Boolean\n"
              << "      -  8 for Date\n"
              << "      -  9 for DateNumber\n"
              << "      -  10 for Timestamp\n"
              << "      -  11 for TimestampNumber\n";
    return 1;
  }

  try {

    switch (std::stoi(argv[1])) {

    case StrykeType::Long: {
      auto result = stryke::orcReader<stryke::Long>(argv[2]);
      for (auto &&i : result) {
        std::cout << "value :" << std::get<0>(i).data << std::endl;
      }
    } break;
    case StrykeType::Short: {
      auto result = stryke::orcReader<stryke::Short>(argv[2]);
      for (auto &&i : result) {
        std::cout << "value :" << std::get<0>(i).data << std::endl;
      }
    } break;
    case StrykeType::Int: {
      auto result = stryke::orcReader<stryke::Int>(argv[2]);
      for (auto &&i : result) {
        std::cout << "value :" << std::get<0>(i).data << std::endl;
      }
    } break;
    case StrykeType::String: {
      auto result = stryke::orcReader<stryke::String>(argv[2]);
      for (auto &&i : result) {
        std::cout << "value :" << std::get<0>(i).data << std::endl;
      }
    } break;
    case StrykeType::Double: {
      auto result = stryke::orcReader<stryke::Double>(argv[2]);
      for (auto &&i : result) {
        std::cout << "value :" << std::get<0>(i).data << std::endl;
      }
    } break;
    case StrykeType::Float: {
      auto result = stryke::orcReader<stryke::Float>(argv[2]);
      for (auto &&i : result) {
        std::cout << "value :" << std::get<0>(i).data << std::endl;
      }
    } break;
    case StrykeType::Boolean: {
      auto result = stryke::orcReader<stryke::Boolean>(argv[2]);
      for (auto &&i : result) {
        std::cout << "value :" << std::get<0>(i).data << std::endl;
      }
    } break;
    case StrykeType::Date: {
      auto result = stryke::orcReader<stryke::Date>(argv[2]);
      for (auto &&i : result) {
        std::cout << "value :" << std::get<0>(i).data << std::endl;
      }
    } break;
    case StrykeType::DateNumber: {
      auto result = stryke::orcReader<stryke::DateNumber>(argv[2]);
      for (auto &&i : result) {
        std::cout << "value :" << std::get<0>(i).data << std::endl;
      }
    } break;
    case StrykeType::Timestamp: {
      auto result = stryke::orcReader<stryke::Timestamp>(argv[2]);
      for (auto &&i : result) {
        std::cout << "value :" << std::get<0>(i).data << std::endl;
      }
    } break;
    case StrykeType::TimestampNumber: {
      auto result = stryke::orcReader<stryke::TimestampNumber>(argv[2]);
      for (auto &&i : result) {
        std::cout << "value :" << std::get<0>(i).data << std::endl;
      }
    } break;
    default:
      break;
    }

  } catch (std::exception &ex) {
    std::cerr << "Caught exception: " << ex.what() << "\n";
    return 1;
  }
  return 0;
}
