//
//  Stryke
//
//  https://github.com/edmBernard/Stryke
//
//  Created by Erwan BERNARD on 22/12/2018.
//
//  Copyright (c) 2018, 2019 Erwan BERNARD. All rights reserved.
//  Distributed under the Apache License, Version 2.0. (See accompanying
//  file LICENSE or copy at http://www.apache.org/licenses/LICENSE-2.0)
//
#pragma once
#ifndef STRYKE_READER_HPP_
#define STRYKE_READER_HPP_

#include "date/date.h"
#include "orc/OrcFile.hh"
#include "orc/Type.hh"
#include "stryke/type.hpp"

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
inline stryke::Long get_value<stryke::Long>(orc::StructVectorBatch *structBatch, uint64_t index_col, uint64_t index_row) {
  orc::LongVectorBatch *col1 = dynamic_cast<orc::LongVectorBatch *>(structBatch->fields[index_col]);
  return col1->data[index_row];
}
template <>
inline stryke::Short get_value<stryke::Short>(orc::StructVectorBatch *structBatch, uint64_t index_col, uint64_t index_row) {
  orc::LongVectorBatch *col1 = dynamic_cast<orc::LongVectorBatch *>(structBatch->fields[index_col]);
  return col1->data[index_row];
}
template <>
inline stryke::Int get_value<stryke::Int>(orc::StructVectorBatch *structBatch, uint64_t index_col, uint64_t index_row) {
  orc::LongVectorBatch *col1 = dynamic_cast<orc::LongVectorBatch *>(structBatch->fields[index_col]);
  return col1->data[index_row];
}

template <>
inline stryke::String get_value<stryke::String>(orc::StructVectorBatch *structBatch, uint64_t index_col, uint64_t index_row) {
  orc::StringVectorBatch *col1 = dynamic_cast<orc::StringVectorBatch *>(structBatch->fields[index_col]);
  return std::string(col1->data[index_row], col1->length[index_row]);
}

template <>
inline stryke::Double get_value<stryke::Double>(orc::StructVectorBatch *structBatch, uint64_t index_col, uint64_t index_row) {
  orc::DoubleVectorBatch *col1 = dynamic_cast<orc::DoubleVectorBatch *>(structBatch->fields[index_col]);
  return col1->data[index_row];
}
template <>
inline stryke::Float get_value<stryke::Float>(orc::StructVectorBatch *structBatch, uint64_t index_col, uint64_t index_row) {
  orc::DoubleVectorBatch *col1 = dynamic_cast<orc::DoubleVectorBatch *>(structBatch->fields[index_col]);
  return col1->data[index_row];
}

template <>
inline stryke::Boolean get_value<stryke::Boolean>(orc::StructVectorBatch *structBatch, uint64_t index_col, uint64_t index_row) {
  orc::LongVectorBatch *col1 = dynamic_cast<orc::LongVectorBatch *>(structBatch->fields[index_col]);
  return col1->data[index_row];
}

template <>
inline stryke::Date get_value<stryke::Date>(orc::StructVectorBatch *structBatch, uint64_t index_col, uint64_t index_row) {
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
inline stryke::DateNumber get_value<stryke::DateNumber>(orc::StructVectorBatch *structBatch, uint64_t index_col, uint64_t index_row) {
  orc::LongVectorBatch *date = dynamic_cast<orc::LongVectorBatch *>(structBatch->fields[index_col]);
  return date->data[index_row];
}

template <>
inline stryke::Timestamp get_value<stryke::Timestamp>(orc::StructVectorBatch *structBatch, uint64_t index_col, uint64_t index_row) {
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
inline stryke::TimestampNumber get_value<stryke::TimestampNumber>(orc::StructVectorBatch *structBatch, uint64_t index_col, uint64_t index_row) {
  orc::TimestampVectorBatch *date = dynamic_cast<orc::TimestampVectorBatch *>(structBatch->fields[index_col]);
  return date->data[index_row] + date->nanoseconds[index_row] / 1000000000.;
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


//! OrcReader work like an iterator, it must be reset to get data another time.
//!
//! \tparam Types
//!
template <typename... Types>
class OrcReader {
  std::string filename;
  orc::ReaderOptions options;
  std::unique_ptr<orc::Reader> reader;
  std::unique_ptr<orc::RowReader> rowReader;
  uint64_t batch_size;
  std::unique_ptr<orc::ColumnVectorBatch> batch;

public:
  OrcReader(std::string filename, uint64_t batch_size = 10000) : filename(filename), batch_size(batch_size) {
    this->reader = orc::createReader(orc::readLocalFile(this->filename), this->options);
    this->rowReader = this->reader->createRowReader();
    this->batch = this->rowReader->createRowBatch(this->batch_size);
  }

  std::array<std::string, sizeof...(Types)> get_cols_name() {

    std::array<std::string, sizeof...(Types)> ret;

    for (std::size_t i = 0; i < sizeof...(Types); ++i) {
      ret[i] = this->reader->getType().getFieldName(i);
    }

    return ret;
  }

  void reset() {
    this->reader = orc::createReader(orc::readLocalFile(this->filename), this->options);
    this->rowReader = this->reader->createRowReader();
    this->batch = this->rowReader->createRowBatch(this->batch_size);
  }

  std::vector<std::tuple<Types...>> get_data() {
    std::vector<std::tuple<Types...>> output;

    while (rowReader->next(*batch)) {
      orc::StructVectorBatch *structBatch = dynamic_cast<orc::StructVectorBatch *>(batch.get());

      for (uint64_t r = 0; r < batch->numElements; ++r) {
        output.push_back(utils::for_each<Types...>(structBatch, r));
      }
    }
    return output;
  }

  std::vector<std::tuple<Types...>> get_batch() {
    std::vector<std::tuple<Types...>> output;

    if (rowReader->next(*batch)) {
      orc::StructVectorBatch *structBatch = dynamic_cast<orc::StructVectorBatch *>(batch.get());

      for (uint64_t r = 0; r < batch->numElements; ++r) {
        output.push_back(utils::for_each<Types...>(structBatch, r));
      }
    }

    return output;
  }

  // void get_stripe_stats() {
  //   for (int i = 0; i < this->reader->getNumberOfStripes; ++i) {
  //     auto tmp = this->reader->getStripe(i);

  //   }
  // }

};

} // namespace stryke

#endif // !STRYKE_READER_HPP_
