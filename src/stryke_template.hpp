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

namespace stryke {

// All Type
// orc::BYTE        --> Not Impletmented
// orc::INT
// orc::SHORT
// orc::LONG
// orc::STRING
// orc::CHAR
// orc::VARCHAR     --> Not Impletmented
// orc::BINARY      --> Not Impletmented
// orc::FLOAT
// orc::DOUBLE
// orc::DECIMAL     --> Not Impletmented
// orc::BOOLEAN
// orc::DATE
// orc::TIMESTAMP
// orc::STRUCT      --> Not Impletmented
// orc::LIST        --> Not Impletmented
// orc::MAP         --> Not Impletmented
// orc::UNION       --> Not Impletmented

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

// Long Type Category
class Long {
public:
  Long(long &&data)
      : data(std::move(data)) {
  }
  long data;
  typedef Long type;
};
class Short {
public:
  Short(short &&data)
      : data(std::move(data)) {
  }
  short data;
  typedef Long type;
};
class Int {
public:
  Int(int &&data)
      : data(std::move(data)) {
  }
  int data;
  typedef Long type;
};

// String Type Category
class String {
public:
  String(std::string &&data)
      : data(std::move(data)) {
  }
  std::string data;
  typedef String type;
};
class Char {
public:
  Char(char &&data)
      : data(std::move(data)) {
  }
  char data;
  typedef String type;
};

// Double Type Category
class Double {
public:
  Double(double &&data)
      : data(std::move(data)) {
  }
  double data;
  typedef Double type;
};
class Float {
public:
  Float(float &&data)
      : data(std::move(data)) {
  }
  float data;
  typedef Double type;
};

// Boolean Type Category
class Boolean {
public:
  Boolean(bool &&data)
      : data(std::move(data)) {
  }
  bool data;
  typedef Boolean type;
};

// Date Type Category
class Date {
public:
  Date(std::string &&data)
      : data(std::move(data)) {
  }
  std::string data;
  typedef Date type;
};

// Timestamp Type Category
class Timestamp {
public:
  Timestamp(std::string &&data)
      : data(std::move(data)) {
  }
  std::string data;
  typedef Timestamp type;
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
      auto col = std::get<N>(data[i]);
      std::cout << col.data << " - " << std::endl;
      if (col.data == 0) {
        longBatch->notNull[i] = 0;
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
    std::cout << "debugafter" << std::endl;
    tsBatch->hasNulls = hasNull;
    tsBatch->numElements = numValues;
    return true;
  }
};

namespace utils {

template <typename T, std::size_t... Indices>
auto fillValuesImpl(std::index_sequence<Indices...>, std::vector<T> &data, orc::StructVectorBatch *structBatch, uint64_t numValues) -> std::vector<bool> {
  // return {Filler<T, Indices, decltype(std::get<Indices>(data[0]))>::fillValue(data, structBatch, numValues)...};
  return {Filler<T, Indices, typename std::tuple_element<Indices, T>::type::type>::fillValue(data, structBatch, numValues)...};
}

template <typename... Types>
auto fillValues(std::vector<std::tuple<Types...>> &data, orc::StructVectorBatch *structBatch, uint64_t numValues) {
  return fillValuesImpl(std::index_sequence_for<Types...>(), data, structBatch, numValues);
}

} // namespace utils

// I don't make implementation of the default template to raise on error at compile time if it's not implemented
template <typename T>
bool addStructField(std::unique_ptr<orc::Type> &struct_type);

template <>
bool addStructField<Int>(std::unique_ptr<orc::Type> &struct_type) {
  static int count = 0;
  struct_type->addStructField("int_" + std::to_string(count++), orc::createPrimitiveType(orc::TypeKind::INT));
  return true;
}

template <>
bool addStructField<Long>(std::unique_ptr<orc::Type> &struct_type) {
  static int count = 0;
  struct_type->addStructField("date_" + std::to_string(count++), orc::createPrimitiveType(orc::TypeKind::LONG));
  return true;
}

template <>
bool addStructField<Date>(std::unique_ptr<orc::Type> &struct_type) {
  static int count = 0;
  struct_type->addStructField("date_" + std::to_string(count++), orc::createPrimitiveType(orc::TypeKind::DATE));
  return true;
}

template <>
bool addStructField<Timestamp>(std::unique_ptr<orc::Type> &struct_type) {
  static int count = 0;
  struct_type->addStructField("timstamp_" + std::to_string(count++), orc::createPrimitiveType(orc::TypeKind::TIMESTAMP));
  return true;
}

template <typename... Types>
std::vector<bool> create_schema(std::unique_ptr<orc::Type> &struct_type) {
  return {addStructField<Types>(struct_type)...};
}

//! Writer in one file one thread.
//!
//!
template <typename... Types>
class OrcWriterImpl {
public:
  OrcWriterImpl(uint64_t batchSize, int batchNb_max, std::string folder, std::string prefix)
      : batchSize(batchSize) {

    fileType = orc::createStructType();
    auto ret = create_schema<Types...>(fileType);

    for (auto &&i : ret) {
      std::cout << i << std::endl;
    }

    options.setStripeSize(1000);

    this->outStream = orc::writeLocalFile(folder + "/" + prefix);
    this->writer = orc::createWriter(*fileType, outStream.get(), options);
    this->rowBatch = this->writer->createRowBatch(this->batchSize);
  }

  ~OrcWriterImpl() {
    addToFile();
    std::cout << "Destrutor" << std::endl;
    this->writer->close();
  }

  void write(Types... dataT) {

    // std::cout << "argument length: " << sizeof...(data) << std::endl;
    if (this->numValues < this->batchSize) {

      this->data.push_back(std::make_tuple(dataT...));
      ++this->numValues;

    } else {
      addToFile();
    }
  }

  void addToFile() {
    orc::StructVectorBatch *structBatch = dynamic_cast<orc::StructVectorBatch *>(this->rowBatch.get());
    structBatch->numElements = this->numValues;

    auto ret = utils::fillValues(this->data, structBatch, this->numValues);

    std::cout << "ret : FillValues :" << std::endl;
    for (auto &&i : ret) {
      std::cout << i << std::endl;
    }

    this->writer->add(*this->rowBatch);

    this->data.clear();
    this->numValues = 0;
  }

  OrcWriterImpl &setSchema() {
    std::cout << "Apply a schema" << std::endl;
    return *this;
  }

private:
  std::unique_ptr<orc::Type> fileType;
  orc::WriterOptions options;
  std::unique_ptr<orc::OutputStream> outStream;
  std::unique_ptr<orc::Writer> writer;
  std::unique_ptr<orc::ColumnVectorBatch> rowBatch;

  std::vector<std::tuple<Types...>> data; // buffer that holds a batch of rows in tuple

  uint64_t numValues = 0; // num of lines read in a batch
  uint64_t batchSize;
};

//! Writer in mutli file one thread.
//!
//!
template <typename... Types>
class OrcWriterMulti {
public:
  OrcWriterMulti(uint64_t batchSize, int batchNb_max, std::string folder, std::string prefix) {
  }

  void write(Types... data) {
    this->fifo.push(data...);
  }
};

//! Writer in mutli file separate thread.
//!
//!
template <typename... Types>
class OrcWriter {
public:
  OrcWriter(uint64_t batchSize, int batchNb_max, std::string folder, std::string prefix) {
  }

  void write(Types... data) {
    this->fifo.push(data...);
  }

  std::tuple<Types...> get() {
    return this->fifo.pop();
  }

  std::queue<std::tuple<Types...>> fifo;
  std::thread writer_thread;
};

} // namespace stryke

#endif // !STRYKE_HPP_
