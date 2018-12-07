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

// Type
class Int {
  public:
    Int(int&& data) : data(std::move(data)) {}
    int data;
};

class Date {
  public:
    Date(long&& data) : data(std::move(data)) {}
    long data;
};

class Long {
  public:
    Long(long&& data) : data(std::move(data)) {}
    long data;
};

class String {
  public:
    String(std::string&& data) : data(std::move(data)) {}
    std::string data;
};


// I use a class because function template partial specialisation is not allowed in c++, but class yes
template<typename Types, uint64_t N, typename T>
class Filler {
public:
  static bool fillValue(const std::vector<Types> &data,
                orc::StructVectorBatch *batch,
                uint64_t numValues) {
    return false;
  }

};

template<typename Types, uint64_t N>
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

template<typename Types, uint64_t N>
class Filler<Types, N, Date> {
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

template<typename Types, uint64_t N>
class Filler<Types, N, Int> {
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


namespace utils {

template <typename T, std::size_t... Indices>
auto fillValuesImpl(std::index_sequence<Indices...>, std::vector<T> &data, orc::StructVectorBatch *structBatch, uint64_t numValues) -> std::vector<bool> {
  // return {Filler<T, Indices, decltype(std::get<Indices>(data[0]))>::fillValue(data, structBatch, numValues)...};
  return {Filler<T, Indices, typename std::tuple_element<Indices, T>::type>::fillValue(data, structBatch, numValues)...};
}

template <typename... Types>
auto fillValues(std::vector<std::tuple<Types...>> &data, orc::StructVectorBatch *structBatch, uint64_t numValues) {
  return fillValuesImpl(std::index_sequence_for<Types...>(), data, structBatch, numValues);
}

} // namespace utils

template <typename T>
bool addStructField(std::unique_ptr<orc::Type> &struct_type) {
  return false;
}

template <>
bool addStructField<Int>(std::unique_ptr<orc::Type> &struct_type) {
  static int count = 0;
  struct_type->addStructField("col_int_" + std::to_string(count++), orc::createPrimitiveType(orc::TypeKind::INT));
  return true;
}

template <>
bool addStructField<Date>(std::unique_ptr<orc::Type> &struct_type) {
  static int count = 0;
  struct_type->addStructField("col_date_" + std::to_string(count++), orc::createPrimitiveType(orc::TypeKind::DATE));
  return true;
}

template <>
bool addStructField<Long>(std::unique_ptr<orc::Type> &struct_type) {
  static int count = 0;
  struct_type->addStructField("col_date_" + std::to_string(count++), orc::createPrimitiveType(orc::TypeKind::LONG));
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
