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
#pragma once
#ifndef STRYKE_READER_HPP_
#define STRYKE_READER_HPP_

#include "date/date.h"
#include "orc/OrcFile.hh"
#include "orc/Type.hh"
#include "stryke_template.hpp"

#include <iostream>
#include <sstream>
#include <string>
#include <vector>

namespace stryke {

namespace fs = std::filesystem;

// Long
// Short
// Int
// String
// Double
// Float
// Boolean
// Date
// DateNumber
// Timestamp
// TimestampNumber

template <typename T>
T get_value(orc::StructVectorBatch *structBatch, uint64_t index_col, uint64_t index_row);

template <>
stryke::Long get_value<stryke::Long>(orc::StructVectorBatch *structBatch, uint64_t index_col, uint64_t index_row) {
  orc::LongVectorBatch *col1 = dynamic_cast<orc::LongVectorBatch *>(structBatch->fields[index_col]);
  return col1->data[index_row];
}
template <>
stryke::Short get_value<stryke::Short>(orc::StructVectorBatch *structBatch, uint64_t index_col, uint64_t index_row) {
  orc::LongVectorBatch *col1 = dynamic_cast<orc::LongVectorBatch *>(structBatch->fields[index_col]);
  return col1->data[index_row];
}
template <>
stryke::Int get_value<stryke::Int>(orc::StructVectorBatch *structBatch, uint64_t index_col, uint64_t index_row) {
  orc::LongVectorBatch *col1 = dynamic_cast<orc::LongVectorBatch *>(structBatch->fields[index_col]);
  return col1->data[index_row];
}

template <>
stryke::String get_value<stryke::String>(orc::StructVectorBatch *structBatch, uint64_t index_col, uint64_t index_row) {
  orc::StringVectorBatch *col1 = dynamic_cast<orc::StringVectorBatch *>(structBatch->fields[index_col]);
  return std::string(col1->data[index_row], col1->length[index_row]);
}

template <>
stryke::Double get_value<stryke::Double>(orc::StructVectorBatch *structBatch, uint64_t index_col, uint64_t index_row) {
  orc::DoubleVectorBatch *col1 = dynamic_cast<orc::DoubleVectorBatch *>(structBatch->fields[index_col]);
  return col1->data[index_row];
}
template <>
stryke::Float get_value<stryke::Float>(orc::StructVectorBatch *structBatch, uint64_t index_col, uint64_t index_row) {
  orc::DoubleVectorBatch *col1 = dynamic_cast<orc::DoubleVectorBatch *>(structBatch->fields[index_col]);
  return col1->data[index_row];
}

template <>
stryke::Boolean get_value<stryke::Boolean>(orc::StructVectorBatch *structBatch, uint64_t index_col, uint64_t index_row) {
  orc::LongVectorBatch *col1 = dynamic_cast<orc::LongVectorBatch *>(structBatch->fields[index_col]);
  return col1->data[index_row];
}

template <>
stryke::Date get_value<stryke::Date>(orc::StructVectorBatch *structBatch, uint64_t index_col, uint64_t index_row) {
  orc::LongVectorBatch *date = dynamic_cast<orc::LongVectorBatch *>(structBatch->fields[index_col]);

  date::sys_days tp = date::floor<date::days>(std::chrono::system_clock::time_point()) + date::days(date->data[index_row]);

  auto ymd = date::year_month_day(tp); // calendar date

  std::ostringstream output;
  output << int(ymd.year());
  output << "-" << std::setw(2) << std::setfill('0') << unsigned(ymd.month());
  output << "-" << std::setw(2) << std::setfill('0') << unsigned(ymd.day());

  return output.str();
}

template <>
stryke::DateNumber get_value<stryke::DateNumber>(orc::StructVectorBatch *structBatch, uint64_t index_col, uint64_t index_row) {
  orc::LongVectorBatch *date = dynamic_cast<orc::LongVectorBatch *>(structBatch->fields[index_col]);
  return date->data[index_row];
}

template <>
stryke::Timestamp get_value<stryke::Timestamp>(orc::StructVectorBatch *structBatch, uint64_t index_col, uint64_t index_row) {
  orc::TimestampVectorBatch *date = dynamic_cast<orc::TimestampVectorBatch *>(structBatch->fields[index_col]);

  std::chrono::system_clock::time_point tp = std::chrono::system_clock::from_time_t(date->data[index_row]);

  auto daypoint = date::floor<date::days>(tp);
  auto ymd = date::year_month_day(daypoint); // calendar date
  auto tod = date::make_time(tp - daypoint); // Yields time_of_day type

  std::ostringstream output;
  output << int(ymd.year());
  output << "-" << std::setw(2) << std::setfill('0') << unsigned(ymd.month());
  output << "-" << std::setw(2) << std::setfill('0') << unsigned(ymd.day());
  output << " " << std::setw(2) << std::setfill('0') << tod.hours().count();
  output << ":" << std::setw(2) << std::setfill('0') << tod.minutes().count();
  output << ":" << std::setw(2) << std::setfill('0') << tod.seconds().count();
  if (date->nanoseconds[index_row] > 0) {
    output << "." << date->nanoseconds[index_row];
  }

  return output.str();
}

template <>
stryke::TimestampNumber get_value<stryke::TimestampNumber>(orc::StructVectorBatch *structBatch, uint64_t index_col, uint64_t index_row) {
  orc::TimestampVectorBatch *date = dynamic_cast<orc::TimestampVectorBatch *>(structBatch->fields[index_col]);
  return date->data[index_row];
}

namespace utils {

template <typename T, std::size_t... Indices>
auto for_each_impl(std::index_sequence<Indices...>, orc::StructVectorBatch *structBatch, uint64_t row) -> T {
  return {get_value<typename std::tuple_element<Indices, T>::type>(structBatch, Indices, row)...};
}

template <typename... Types>
auto for_each(orc::StructVectorBatch *structBatch, uint64_t row) -> std::tuple<Types...> {
  return for_each_impl<std::tuple<Types...>>(std::index_sequence_for<Types...>(), structBatch, row);
}

} // namespace utils

template <typename... Types>
std::vector<std::tuple<Types...>> orcReader(std::string filename) {
  std::vector<std::tuple<Types...>> output;

  orc::ReaderOptions options;
  std::unique_ptr<orc::Reader> reader = orc::createReader(orc::readLocalFile(filename), options);
  std::unique_ptr<orc::RowReader> rowReader = reader->createRowReader();
  std::unique_ptr<orc::ColumnVectorBatch> batch = rowReader->createRowBatch(10);

  while (rowReader->next(*batch)) {
    orc::StructVectorBatch *structBatch = dynamic_cast<orc::StructVectorBatch *>(batch.get());

    for (uint64_t r = 0; r < batch->numElements; ++r) {
      output.push_back(utils::for_each<Types...>(structBatch, r));
    }
  }
  return output;
}

} // namespace stryke

#endif // !STRYKE_READER_HPP_
