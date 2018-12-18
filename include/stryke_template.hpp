//
//  Stryke
//
//  https://github.com/edmBernard/Stryke
//
//  Created by Erwan BERNARD on 02/12/2018.
//
//  Copyright (c) 2018 Erwan BERNARD. All rights reserved.
//  Distributed under the Apache License, Version 2.0. (See accompanying
//  file LICENSE or copy at http://www.apache.org/licenses/LICENSE-2.0)
//
#pragma once
#ifndef STRYKE_HPP_
#define STRYKE_HPP_

#include "orc/OrcFile.hh"
#include "orc/Type.hh"

#include <cmath>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <memory>
#include <queue>
#include <regex>
#include <string>
#include <thread>
#include <tuple>

namespace stryke {

class WriterOptions {
public:
  WriterOptions() {
  }

  void disable_lock_file(bool disable_lock_file = true) {
    this->create_lock_file = !disable_lock_file;
  }

  void set_batch_size(uint64_t batchSize) {
    this->batchSize = batchSize;
  }

  void set_batch_max(int nbr_batch_max) {
    this->nbr_batch_max = nbr_batch_max;
  }

  void set_stripe_size(uint64_t stripeSize) {
    this->stripeSize = stripeSize;
  }

  void set_cron(int minutes) {
    this->cron = minutes;
  }

  bool create_lock_file = true;
  uint64_t batchSize = 10000;
  int nbr_batch_max = 0;
  uint64_t stripeSize = 10000;
  int cron = -1;
};

// ==============================================================
// Type Implementation
// ==============================================================

// All Type
// orc::BYTE        --> Not Implemented
// orc::INT
// orc::SHORT
// orc::LONG
// orc::STRING
// orc::CHAR        --> Not Implemented
// orc::VARCHAR     --> Not Implemented
// orc::BINARY      --> Not Implemented
// orc::FLOAT
// orc::DOUBLE
// orc::DECIMAL     --> Not Implemented
// orc::BOOLEAN
// orc::DATE
// orc::TIMESTAMP
// orc::STRUCT      --> Not Implemented
// orc::LIST        --> Not Implemented
// orc::MAP         --> Not Implemented
// orc::UNION       --> Not Implemented

// Long Type Category
class Long {
public:
  Long(long data)
      : data(data), empty(false) {
  }
  Long() {
  }
  long data;
  bool empty = true;
  typedef Long type;
  static const orc::TypeKind TypeKind = orc::TypeKind::LONG;
};

class Short {
public:
  Short(short data)
      : data(data), empty(false) {
  }
  Short() {
  }
  short data;
  bool empty = true;
  typedef Long type;
  static const orc::TypeKind TypeKind = orc::TypeKind::SHORT;
};

class Int {
public:
  Int(int data)
      : data(data), empty(false) {
  }
  Int() {
  }
  int data;
  bool empty = true;
  typedef Long type;
  static const orc::TypeKind TypeKind = orc::TypeKind::INT;
};

// String Type Category
class String {
public:
  String(std::string &&data)
      : data(std::move(data)) {
  }
  String(const char *data)
      : data(std::string(data)) {
  }
  String() {
  }
  std::string data;
  typedef String type;
  static const orc::TypeKind TypeKind = orc::TypeKind::STRING;
};

// Not working
// class Char {
// public:
//   Char(char data)
//       : data(1, data) {
//   }
//   Char() {
//   }
//   std::string data;
//   typedef String type;
//   static const orc::TypeKind TypeKind = orc::TypeKind::CHAR;
// };

// Double Type Category
class Double {
public:
  Double(double data)
      : data(data), empty(false) {
  }
  Double() {
  }
  double data;
  bool empty = true;
  typedef Double type;
  static const orc::TypeKind TypeKind = orc::TypeKind::DOUBLE;
};

class Float {
public:
  Float(float data)
      : data(data), empty(false) {
  }
  Float() {
  }
  float data;
  bool empty = true;
  typedef Double type;
  static const orc::TypeKind TypeKind = orc::TypeKind::FLOAT;
};

// Boolean Type Category
class Boolean {
public:
  Boolean(bool data)
      : data(data), empty(false) {
  }
  Boolean() {
  }
  bool data;
  bool empty = true;
  typedef Boolean type;
  static const orc::TypeKind TypeKind = orc::TypeKind::BOOLEAN;
};

// Date Type Category
class Date {
public:
  Date(std::string &&data)
      : data(std::move(data)) {
  }
  Date(const char *data)
      : data(std::string(data)) {
  }
  Date() {
  }
  std::string data;
  typedef Date type;
  static const orc::TypeKind TypeKind = orc::TypeKind::DATE;
};

class DateNumber {
public:
  DateNumber(long data)
      : data(data), empty(false) {
  }
  DateNumber() {
  }
  long data;
  bool empty = true;
  typedef Long type;
  static const orc::TypeKind TypeKind = orc::TypeKind::DATE;
};

// Timestamp Type Category
class Timestamp {
public:
  Timestamp(std::string &&data)
      : data(std::move(data)) {
  }
  Timestamp() {
  }
  Timestamp(const char *data)
      : data(std::string(data)) {
  }
  std::string data;
  typedef Timestamp type;
  static const orc::TypeKind TypeKind = orc::TypeKind::TIMESTAMP;
};

class TimestampNumber {
public:
  TimestampNumber(double data)
      : data(data), empty(false) {
  }
  TimestampNumber() {
  }
  double data;
  bool empty = true;
  typedef TimestampNumber type;
  static const orc::TypeKind TypeKind = orc::TypeKind::TIMESTAMP;
};

namespace utils {

// ==============================================================
// Filler Implementation: function to fill batch
// ==============================================================

// I use a class because function template partial specialisation is not allowed in c++, but class yes
template <typename Types, uint64_t N, typename T>
class Filler {
public:
  // This function is commented to throw on exception at compile time if one of requested type is not implemented
  // static bool fillValue(const std::vector<Types> &data,
  //                       orc::StructVectorBatch *batch,
  //                       uint64_t numValues) {
  //   return false;
  // }
};

template <typename Types, uint64_t N>
class Filler<Types, N, Long> {
public:
  static bool fillValue(const std::vector<Types> &data,
                        orc::StructVectorBatch *batch,
                        uint64_t numValues,
                        std::unique_ptr<orc::DataBuffer<char>> &string_buffer,
                        uint64_t &string_buffer_offset) {
    orc::LongVectorBatch *longBatch = dynamic_cast<orc::LongVectorBatch *>(batch->fields[N]);
    bool hasNull = false;
    for (uint64_t i = 0; i < numValues; ++i) {
      auto col = std::get<N>(data[i]);
      if (col.empty) {
        batch->notNull[i] = 0;
        hasNull = true;
      } else {
        longBatch->notNull[i] = 1;
        longBatch->data[i] = col.data;
      }
    }
    longBatch->hasNulls = hasNull;
    longBatch->numElements = numValues;
    return true;
  }
};

template <typename Types, uint64_t N>
class Filler<Types, N, String> {
public:
  static bool fillValue(const std::vector<Types> &data,
                        orc::StructVectorBatch *batch,
                        uint64_t numValues,
                        std::unique_ptr<orc::DataBuffer<char>> &string_buffer,
                        uint64_t &string_buffer_offset) {
    orc::StringVectorBatch *stringBatch = dynamic_cast<orc::StringVectorBatch *>(batch->fields[N]);
    bool hasNull = false;
    for (uint64_t i = 0; i < numValues; ++i) {
      std::string col = std::get<N>(data[i]).data;
      if (col.empty()) {
        batch->notNull[i] = 0;
        hasNull = true;
      } else {
        batch->notNull[i] = 1;
        if (string_buffer->size() - string_buffer_offset < col.size()) {
          string_buffer->reserve(string_buffer->size() * 2);
        }
        memcpy(string_buffer->data() + string_buffer_offset,
              col.c_str(),
              col.size());
        stringBatch->data[i] = string_buffer->data() + string_buffer_offset;
        stringBatch->length[i] = static_cast<int64_t>(col.size());
        string_buffer_offset += col.size();
      }
    }
    stringBatch->hasNulls = hasNull;
    stringBatch->numElements = numValues;
    return true;
  }
};

template <typename Types, uint64_t N>
class Filler<Types, N, Double> {
public:
  static bool fillValue(const std::vector<Types> &data,
                        orc::StructVectorBatch *batch,
                        uint64_t numValues,
                        std::unique_ptr<orc::DataBuffer<char>> &string_buffer,
                        uint64_t &string_buffer_offset) {
    orc::DoubleVectorBatch *dblBatch = dynamic_cast<orc::DoubleVectorBatch *>(batch->fields[N]);
    bool hasNull = false;
    for (uint64_t i = 0; i < numValues; ++i) {
      auto col = std::get<N>(data[i]);
      if (col.empty) {
        batch->notNull[i] = 0;
        hasNull = true;
      } else {
        dblBatch->notNull[i] = 1;
        dblBatch->data[i] = col.data;
      }
    }
    dblBatch->hasNulls = hasNull;
    dblBatch->numElements = numValues;
    return true;
  }
};

template <typename Types, uint64_t N>
class Filler<Types, N, Boolean> {
public:
  static bool fillValue(const std::vector<Types> &data,
                        orc::StructVectorBatch *batch,
                        uint64_t numValues,
                        std::unique_ptr<orc::DataBuffer<char>> &string_buffer,
                        uint64_t &string_buffer_offset) {
    orc::LongVectorBatch *boolBatch = dynamic_cast<orc::LongVectorBatch *>(batch->fields[N]);
    bool hasNull = false;
    for (uint64_t i = 0; i < numValues; ++i) {
      auto col = std::get<N>(data[i]);
      if (col.empty) {
        batch->notNull[i] = 0;
        hasNull = true;
      } else {
        boolBatch->notNull[i] = 1;
        boolBatch->data[i] = col.data;
      }
    }
    boolBatch->hasNulls = hasNull;
    boolBatch->numElements = numValues;
    return true;
  }
};

template <typename Types, uint64_t N>
class Filler<Types, N, Date> {
public:
  static bool fillValue(const std::vector<Types> &data,
                        orc::StructVectorBatch *batch,
                        uint64_t numValues,
                        std::unique_ptr<orc::DataBuffer<char>> &string_buffer,
                        uint64_t &string_buffer_offset) {
    orc::LongVectorBatch *longBatch = dynamic_cast<orc::LongVectorBatch *>(batch->fields[N]);
    bool hasNull = false;
    for (uint64_t i = 0; i < numValues; ++i) {
      std::string col = std::get<N>(data[i]).data;
      if (col.empty()) {
        batch->notNull[i] = 0;
        hasNull = true;
      } else {
        batch->notNull[i] = 1;
        struct tm tm;
        memset(&tm, 0, sizeof(struct tm));
        strptime(col.c_str(), "%Y-%m-%d", &tm);
        time_t t = timegm(&tm);
        time_t t1970 = 0;
        double seconds = difftime(t, t1970);
        int64_t days = static_cast<int64_t>(seconds / (60 * 60 * 24));
        longBatch->data[i] = days;
      }
    }
    longBatch->hasNulls = hasNull;
    longBatch->numElements = numValues;
    return true;
  }
};

template <typename Types, uint64_t N>
class Filler<Types, N, DateNumber> {
public:
  static bool fillValue(const std::vector<Types> &data,
                        orc::StructVectorBatch *batch,
                        uint64_t numValues,
                        std::unique_ptr<orc::DataBuffer<char>> &string_buffer,
                        uint64_t &string_buffer_offset) {
    orc::LongVectorBatch *longBatch = dynamic_cast<orc::LongVectorBatch *>(batch->fields[N]);
    bool hasNull = false;
    for (uint64_t i = 0; i < numValues; ++i) {
      auto col = std::get<N>(data[i]);
      if (col.empty) {
        batch->notNull[i] = 0;
        hasNull = true;
      } else {
        batch->notNull[i] = 1;
        longBatch->data[i] = int64_t(col.data);
      }
    }
    longBatch->hasNulls = hasNull;
    longBatch->numElements = numValues;
    return true;
  }
};

template <typename Types, uint64_t N>
class Filler<Types, N, Timestamp> {
public:
  static bool fillValue(const std::vector<Types> &data,
                        orc::StructVectorBatch *batch,
                        uint64_t numValues,
                        std::unique_ptr<orc::DataBuffer<char>> &string_buffer,
                        uint64_t &string_buffer_offset) {
    struct tm timeStruct;
    orc::TimestampVectorBatch *tsBatch = dynamic_cast<orc::TimestampVectorBatch *>(batch->fields[N]);
    bool hasNull = false;
    for (uint64_t i = 0; i < numValues; ++i) {
      std::string col = std::get<N>(data[i]).data;
      if (col.empty()) {
        batch->notNull[i] = 0;
        hasNull = true;
      } else {
        memset(&timeStruct, 0, sizeof(timeStruct));
        char *left = strptime(col.c_str(), "%Y-%m-%d %H:%M:%S", &timeStruct);
        if (left == nullptr) {
          batch->notNull[i] = 0;
        } else {
          batch->notNull[i] = 1;
          tsBatch->data[i] = timegm(&timeStruct);
          char *tail;
          double d = strtod(left, &tail);
          if (tail != left) {
            tsBatch->nanoseconds[i] = static_cast<long>(d * 1000000000.0);
          } else {
            tsBatch->nanoseconds[i] = 0;
          }
        }
      }
    }
    tsBatch->hasNulls = hasNull;
    tsBatch->numElements = numValues;
    return true;
  }
};

template <typename Types, uint64_t N>
class Filler<Types, N, TimestampNumber> {
public:
  static bool fillValue(const std::vector<Types> &data,
                        orc::StructVectorBatch *batch,
                        uint64_t numValues,
                        std::unique_ptr<orc::DataBuffer<char>> &string_buffer,
                        uint64_t &string_buffer_offset) {
    orc::TimestampVectorBatch *tsBatch = dynamic_cast<orc::TimestampVectorBatch *>(batch->fields[N]);
    bool hasNull = false;
    for (uint64_t i = 0; i < numValues; ++i) {
      auto col = std::get<N>(data[i]);
      if (col.empty) {
        batch->notNull[i] = 0;
        hasNull = true;
      } else {
        batch->notNull[i] = 1;
        double decimale, integrale;
        decimale = std::modf(col.data, &integrale);
        tsBatch->data[i] = time_t(integrale);
        if (decimale > 0) {
          tsBatch->nanoseconds[i] = static_cast<long>(decimale * 1000000000.0);
        } else {
          tsBatch->nanoseconds[i] = 0;
        }
      }
    }
    tsBatch->hasNulls = hasNull;
    tsBatch->numElements = numValues;
    return true;
  }
};

template <typename T, std::size_t... Indices>
auto fillValuesImpl(std::index_sequence<Indices...>, std::vector<T> &data, orc::StructVectorBatch *structBatch, uint64_t numValues, std::unique_ptr<orc::DataBuffer<char>> &string_buffer, uint64_t &string_buffer_offset) -> std::vector<bool> {
  // return {Filler<T, Indices, decltype(std::get<Indices>(data[0]))>::fillValue(data, structBatch, numValues)...};
  return {Filler<T, Indices, typename std::tuple_element<Indices, T>::type::type>::fillValue(data, structBatch, numValues, string_buffer, string_buffer_offset)...};
}

template <typename... Types>
auto fillValues(std::vector<std::tuple<Types...>> &data, orc::StructVectorBatch *structBatch, uint64_t numValues, std::unique_ptr<orc::DataBuffer<char>> &string_buffer, uint64_t &string_buffer_offset) {
  return fillValuesImpl(std::index_sequence_for<Types...>(), data, structBatch, numValues, string_buffer, string_buffer_offset);
}

// ==============================================================
//  Create Schema Implementation: function to create data schema
// ==============================================================

template <typename T>
bool addStructField(std::unique_ptr<orc::Type> &struct_type, std::string column_name) {
  struct_type->addStructField(column_name, orc::createPrimitiveType(T::TypeKind));
  return true;
}

template <typename... Types, std::size_t... Indices>
auto createSchemaImpl(std::index_sequence<Indices...>, std::unique_ptr<orc::Type> &struct_type, std::array<std::string, sizeof...(Types)> column_names) -> std::vector<bool> {
  // return {Filler<T, Indices, decltype(std::get<Indices>(data[0]))>::fillValue(data, structBatch, numValues)...};
  return {addStructField<Types>(struct_type, column_names[Indices])...};
}

template <typename... Types>
auto createSchema(std::unique_ptr<orc::Type> &struct_type, std::array<std::string, sizeof...(Types)> column_names) {
  return createSchemaImpl<Types...>(std::index_sequence_for<Types...>(), struct_type, column_names);
}

} // namespace utils

// ==============================================================
//  Simple Writer Implementation
// ==============================================================

//! Writer in one file one thread.
//!
//!
template <typename... Types>
class OrcWriterImpl {
public:
  OrcWriterImpl(std::array<std::string, sizeof...(Types)> column_names, std::string filename, const WriterOptions &options)
      : writeroptions(options), column_names(column_names), filename(filename) {

    this->fileType = orc::createStructType();
    auto ret = utils::createSchema<Types...>(this->fileType, this->column_names);

    this->orc_writeroptions.setStripeSize(writeroptions.stripeSize);

    this->outStream = orc::writeLocalFile(this->filename);
    this->writer = orc::createWriter(*this->fileType, outStream.get(), this->orc_writeroptions);
    this->rowBatch = this->writer->createRowBatch(this->writeroptions.batchSize);

    this->string_buffer = std::make_unique<orc::DataBuffer<char>>(*orc::getDefaultPool(), 4 * 1024 * 1024);  // Create buffer for string data. One buffer store all string data

    if (this->writeroptions.create_lock_file) {
      std::ofstream outfile(this->filename + ".lock");
      outfile.close();
    }
  }

  ~OrcWriterImpl() {
    addToFile();
    this->writer->close();
    if (this->writeroptions.create_lock_file) {
      std::filesystem::remove(this->filename + ".lock");
    }
  }

  void write(Types... dataT) {
    this->write_tuple(std::make_tuple(dataT...));
  }

  void write_tuple(std::tuple<Types...> dataT) {

    if (this->numValues >= this->writeroptions.batchSize) {
      addToFile();
    }

    this->data.push_back(dataT);
    ++this->numValues;
  }

  void addToFile() {
    orc::StructVectorBatch *structBatch = dynamic_cast<orc::StructVectorBatch *>(this->rowBatch.get());
    structBatch->numElements = this->numValues;

    auto ret = utils::fillValues(this->data, structBatch, this->numValues, this->string_buffer, this->string_buffer_offset);

    this->writer->add(*this->rowBatch);

    this->data.clear();
    this->numValues = 0;
    this->string_buffer_offset = 0;
  }

private:
  std::unique_ptr<orc::Type> fileType;

  WriterOptions writeroptions;
  orc::WriterOptions orc_writeroptions;

  std::unique_ptr<orc::OutputStream> outStream;
  std::unique_ptr<orc::Writer> writer;
  std::unique_ptr<orc::ColumnVectorBatch> rowBatch;

  std::vector<std::tuple<Types...>> data; // buffer that holds a batch of rows in tuple
  std::array<std::string, sizeof...(Types)> column_names;

  std::unique_ptr<orc::DataBuffer<char>> string_buffer;
  uint64_t string_buffer_offset = 0;

  std::string filename;

  uint64_t numValues = 0; // num of lines read in a batch
};

} // namespace stryke

#endif // !STRYKE_HPP_
