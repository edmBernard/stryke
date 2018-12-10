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

#include <iomanip>
#include <iostream>
#include <memory>
#include <queue>
#include <regex>
#include <string>
#include <thread>
#include <tuple>
#include <cmath>

namespace stryke {

// ==============================================================
// Type Implementation
// ==============================================================

// All Type
// orc::BYTE        --> Not Implemented
// orc::INT
// orc::SHORT
// orc::LONG
// orc::STRING      --> Not Implemented yet coming soon
// orc::CHAR        --> Not Implemented yet coming soon
// orc::VARCHAR     --> Not Implemented
// orc::BINARY      --> Not Implemented
// orc::FLOAT
// orc::DOUBLE
// orc::DECIMAL     --> Not Implemented
// orc::BOOLEAN     --> Not Implemented yet coming soon
// orc::DATE
// orc::TIMESTAMP
// orc::STRUCT      --> Not Implemented
// orc::LIST        --> Not Implemented
// orc::MAP         --> Not Implemented
// orc::UNION       --> Not Implemented

// Long Type Category
class Long {
public:
  Long(long &&data)
      : data(std::move(data)) {
  }
  long data;
  typedef Long type;
  static const orc::TypeKind TypeKind = orc::TypeKind::LONG;
};

class Short {
public:
  Short(short &&data)
      : data(std::move(data)) {
  }
  short data;
  typedef Long type;
  static const orc::TypeKind TypeKind = orc::TypeKind::SHORT;
};

class Int {
public:
  Int(int &&data)
      : data(std::move(data)) {
  }
  int data;
  typedef Long type;
  static const orc::TypeKind TypeKind = orc::TypeKind::INT;
};

// String Type Category
class String {
public:
  String(std::string &&data)
      : data(std::move(data)) {
  }
  std::string data;
  typedef String type;
  static const orc::TypeKind TypeKind = orc::TypeKind::STRING;
};

class Char {
public:
  Char(char &&data)
      : data(std::move(data)) {
  }
  char data;
  typedef String type;
  static const orc::TypeKind TypeKind = orc::TypeKind::CHAR;
};

// Double Type Category
class Double {
public:
  Double(double &&data)
      : data(std::move(data)) {
  }
  double data;
  typedef Double type;
  static const orc::TypeKind TypeKind = orc::TypeKind::DOUBLE;
};

class Float {
public:
  Float(float &&data)
      : data(std::move(data)) {
  }
  float data;
  typedef Double type;
  static const orc::TypeKind TypeKind = orc::TypeKind::FLOAT;
};

// Boolean Type Category
class Boolean {
public:
  Boolean(bool &&data)
      : data(std::move(data)) {
  }
  bool data;
  typedef Boolean type;
  static const orc::TypeKind TypeKind = orc::TypeKind::BOOLEAN;
};

// Date Type Category
class Date {
public:
  Date(std::string &&data)
      : data(std::move(data)) {
  }
  std::string data;
  typedef Date type;
  static const orc::TypeKind TypeKind = orc::TypeKind::DATE;
};

class DateNumber {
public:
  DateNumber(long &&data)
      : data(std::move(data)) {
  }
  long data;
  typedef Long type;
  static const orc::TypeKind TypeKind = orc::TypeKind::DATE;
};

// Timestamp Type Category
class Timestamp {
public:
  Timestamp(std::string &&data)
      : data(std::move(data)) {
  }
  std::string data;
  typedef Timestamp type;
  static const orc::TypeKind TypeKind = orc::TypeKind::TIMESTAMP;
};

class TimestampNumber {
public:
  TimestampNumber(double &&data)
      : data(std::move(data)) {
  }
  double data;
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
                        uint64_t numValues) {
    orc::LongVectorBatch *longBatch = dynamic_cast<orc::LongVectorBatch *>(batch->fields[N]);
    bool hasNull = false;
    for (uint64_t i = 0; i < numValues; ++i) {
      long col = std::get<N>(data[i]).data;
      if (col == 0) {
        longBatch->notNull[i] = 0;
        hasNull = true;
      } else {
        longBatch->notNull[i] = 1;
        longBatch->data[i] = col;
      }
    }
    longBatch->hasNulls = hasNull;
    longBatch->numElements = numValues;
    return true;
  }
};

template <typename Types, uint64_t N>
class Filler<Types, N, Double> {
public:
  static bool fillValue(const std::vector<Types> &data,
                        orc::StructVectorBatch *batch,
                        uint64_t numValues) {
    orc::DoubleVectorBatch *dblBatch = dynamic_cast<orc::DoubleVectorBatch *>(batch->fields[N]);
    bool hasNull = false;
    for (uint64_t i = 0; i < numValues; ++i) {
      double col = std::get<N>(data[i]).data;
      if (col == 0) {
        batch->notNull[i] = 0;
        hasNull = true;
      } else {
        batch->notNull[i] = 1;
        dblBatch->data[i] = col;
      }
    }
    dblBatch->hasNulls = hasNull;
    dblBatch->numElements = numValues;
    return true;
  }
};

template <typename Types, uint64_t N>
class Filler<Types, N, Date> {
public:
  static bool fillValue(const std::vector<Types> &data,
                        orc::StructVectorBatch *batch,
                        uint64_t numValues) {
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
        time_t t = mktime(&tm);
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
                        uint64_t numValues) {
    orc::LongVectorBatch *longBatch = dynamic_cast<orc::LongVectorBatch *>(batch->fields[N]);
    bool hasNull = false;
    for (uint64_t i = 0; i < numValues; ++i) {
      long col = std::get<N>(data[i]).data;
      if (col == 0) {
        batch->notNull[i] = 0;
        hasNull = true;
      } else {
        batch->notNull[i] = 1;
        longBatch->data[i] = int64_t(col);
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
                        uint64_t numValues) {
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
                        uint64_t numValues) {
    orc::TimestampVectorBatch *tsBatch = dynamic_cast<orc::TimestampVectorBatch *>(batch->fields[N]);
    bool hasNull = false;
    for (uint64_t i = 0; i < numValues; ++i) {
      double col = std::get<N>(data[i]).data;
      if (col == 0) {
        batch->notNull[i] = 0;
        hasNull = true;
      } else {
        batch->notNull[i] = 1;
        double decimale, integrale;
        decimale = std::modf(col, &integrale);
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
auto fillValuesImpl(std::index_sequence<Indices...>, std::vector<T> &data, orc::StructVectorBatch *structBatch, uint64_t numValues) -> std::vector<bool> {
  // return {Filler<T, Indices, decltype(std::get<Indices>(data[0]))>::fillValue(data, structBatch, numValues)...};
  return {Filler<T, Indices, typename std::tuple_element<Indices, T>::type::type>::fillValue(data, structBatch, numValues)...};
}

template <typename... Types>
auto fillValues(std::vector<std::tuple<Types...>> &data, orc::StructVectorBatch *structBatch, uint64_t numValues) {
  return fillValuesImpl(std::index_sequence_for<Types...>(), data, structBatch, numValues);
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
  OrcWriterImpl(std::array<std::string, sizeof...(Types)> column_names, std::string filename, uint64_t batchSize = 10000)
      : column_names(column_names), batchSize(batchSize) {

    this->fileType = orc::createStructType();
    auto ret = utils::createSchema<Types...>(this->fileType, this->column_names);

    options.setStripeSize(1000);

    this->outStream = orc::writeLocalFile(filename);
    this->writer = orc::createWriter(*this->fileType, outStream.get(), options);
    this->rowBatch = this->writer->createRowBatch(this->batchSize);
  }

  ~OrcWriterImpl() {
    addToFile();
    this->writer->close();
  }

  void write(Types... dataT) {

    if (this->numValues < this->batchSize) {

      this->data.push_back(std::make_tuple(dataT...));
      ++this->numValues;

    } else {
      addToFile();
    }
  }

  void write(std::tuple<Types...> dataT) {

    if (this->numValues < this->batchSize) {

      this->data.push_back(dataT);
      ++this->numValues;

    } else {
      addToFile();
    }
  }


  void addToFile() {
    orc::StructVectorBatch *structBatch = dynamic_cast<orc::StructVectorBatch *>(this->rowBatch.get());
    structBatch->numElements = this->numValues;

    auto ret = utils::fillValues(this->data, structBatch, this->numValues);

    this->writer->add(*this->rowBatch);

    this->data.clear();
    this->numValues = 0;
  }


private:
  std::unique_ptr<orc::Type> fileType;
  orc::WriterOptions options;
  std::unique_ptr<orc::OutputStream> outStream;
  std::unique_ptr<orc::Writer> writer;
  std::unique_ptr<orc::ColumnVectorBatch> rowBatch;

  std::vector<std::tuple<Types...>> data; // buffer that holds a batch of rows in tuple
  std::array<std::string, sizeof...(Types)> column_names;

  uint64_t numValues = 0; // num of lines read in a batch
  uint64_t batchSize;
};

} // namespace stryke

#endif // !STRYKE_HPP_
